"""
PostgreSQL Load Test Script using Locust
This script replicates the functionality of the JMeter script for testing PostgreSQL
with primary and replica database instances.

Azure best practices applied:
- Connection pooling via psycopg2 pool (shared across users)
- Parameterized queries for security
- Error handling with retry logic
- Secure credential management
- Resource cleanup
"""
import os
import time
import random
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from locust import User, task, constant_throughput, events, between, TaskSet

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("locust_pg_test")

# Default values if environment variables are not set
DEFAULT_CONFIG = {
    "main_threads": "5",
    "main_loops": "10",
    "replica_threads": "20",
    "replica_loops": "20"
}

class PostgreSQLClient:
    """Client for PostgreSQL connections with connection pooling and retry logic"""
    
    def __init__(self, name: str, conn_params, min_conn: int = 5, max_conn: int = 100):
        """
        Initialize PostgreSQL connection pool
        
        Args:
            name: Name identifier for the connection pool
            conn_params: PostgreSQL connection parameters (string or dict)
            min_conn: Minimum connections in the pool
            max_conn: Maximum connections in the pool
        """
        self.name = name
        try:
            # Handle different types of connection parameters
            if isinstance(conn_params, dict):
                # For psycopg2 direct parameters - preferred method for Azure PostgreSQL
                logger.info(f"Creating direct connection pool for {name} using parameter dictionary")
                # Add connection parameters to force TCP
                conn_params_copy = conn_params.copy()
                # Explicitly set host (required for TCP)
                if 'host' not in conn_params_copy or not conn_params_copy['host']:
                    raise ValueError("Host parameter is required for TCP connections")
                # Force SSL for Azure connections
                if 'sslmode' not in conn_params_copy:
                    conn_params_copy['sslmode'] = 'require'
                
                # Create direct connection pool with explicit parameters
                self.connection_pool = pool.ThreadedConnectionPool(
                    minconn=min_conn,
                    maxconn=max_conn,
                    **conn_params_copy  # Pass parameters directly instead of as DSN string
                )
            else:
                # Connection string approach - modify to ensure TCP
                conn_string = conn_params
                if conn_string.startswith('postgresql://'):
                    # Ensure we have query params configured
                    if '?' not in conn_string:
                        conn_string += '?'
                    else:
                        conn_string += '&'
                    # Force TCP by adding 'host=' explicitly to connection string
                    conn_string += 'sslmode=require'
                
                logger.info(f"Creating connection pool for {name} using connection string")
                self.connection_pool = pool.ThreadedConnectionPool(
                    minconn=min_conn,
                    maxconn=max_conn,
                    dsn=conn_string
                )
            logger.info(f"Created connection pool for {name}")
        except Exception as e:
            logger.error(f"Error creating connection pool for {name}: {str(e)}")
            raise
    
    @contextmanager
    def get_connection(self, max_retries: int = 3):
        """
        Get a connection from the pool with retry logic
        
        Args:
            max_retries: Maximum number of connection attempts
        
        Yields:
            PostgreSQL connection object
        """
        retry_count = 0
        conn = None
        
        while retry_count < max_retries:
            try:
                conn = self.connection_pool.getconn()
                yield conn
                break
            except Exception as e:
                retry_count += 1
                wait_time = 0.5 * (2 ** retry_count)  # Exponential backoff
                logger.warning(f"Connection attempt {retry_count} failed: {str(e)}, retrying in {wait_time:.2f}s")
                time.sleep(wait_time)
                if retry_count == max_retries:
                    logger.error(f"Failed to get connection after {max_retries} attempts")
                    raise
            finally:
                if conn is not None:
                    self.connection_pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, commit: bool = True):
        """
        Get a cursor for executing SQL queries
        
        Args:
            commit: Whether to commit the transaction after execution
        
        Yields:
            PostgreSQL cursor object
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                if commit:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database operation failed: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def close(self):
        """Close the connection pool"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()
            logger.info(f"Closed connection pool for {self.name}")

class SharedConnectionManager:
    """Manages shared connection pools for all users"""
    
    def __init__(self):
        """Initialize the connection manager with empty pools"""
        self.main_db = None
        self.replica_db = None
        self.initialized = False
        
    def initialize(self, config, credentials):
        """
        Initialize connection pools for all users
        
        Args:
            config: Configuration dictionary with database settings
            credentials: Dictionary with database credentials
        """
        if self.initialized:
            return
            
        try:
            # Parse connection strings
            main_connection = self._get_connection_string("main", config)
            replica_connection = self._get_connection_string("replica", config)
            
            # Parse host, port, and database for both connections
            main_host, main_port, main_db = self._parse_connection(main_connection)
            replica_host, replica_port, replica_db = self._parse_connection(replica_connection)
            
            # Connection parameters for main database
            main_conn_params = {
                "host": main_host,
                "port": main_port,
                "dbname": main_db,
                "user": credentials['mainuser'],
                "password": credentials['mainpassword'],
                "sslmode": "require"  # Force SSL for Azure PostgreSQL
            }
            
            # Connection parameters for replica database
            replica_conn_params = {
                "host": replica_host,
                "port": replica_port,
                "dbname": replica_db,
                "user": credentials['replicauser'],
                "password": credentials['replicapassword'],
                "sslmode": "require"  # Force SSL for Azure PostgreSQL
            }
            
            # Log connection parameters (with masked passwords)
            masked_main_params = main_conn_params.copy()
            masked_main_params["password"] = "********"
            masked_replica_params = replica_conn_params.copy()
            masked_replica_params["password"] = "********"
            
            logger.info(f"Main connection parameters: {masked_main_params}")
            logger.info(f"Replica connection parameters: {masked_replica_params}")
            
            # Size pools based on expected total user count and thread configuration
            # Using higher values for min_conn as this is shared across all users
            main_threads = config["main_threads"]
            replica_threads = config["replica_threads"]
            
            # Create connection pools with connection parameter dictionaries
            # For shared pools, we increase the pool size based on thread counts
            self.main_db = PostgreSQLClient(
                "main_db", 
                main_conn_params, 
                min_conn=5,  # Higher minimum for shared pool
                max_conn=main_threads * 10  # Scale max connections with thread count
            )
            
            self.replica_db = PostgreSQLClient(
                "replica_db", 
                replica_conn_params, 
                min_conn=5,  # Higher minimum for shared pool
                max_conn=replica_threads * 10  # Scale max connections with thread count
            )
            
            self.initialized = True
            logger.info("Shared database connection pools established successfully")
            
        except Exception as e:
            logger.error(f"Failed to establish shared database connections: {str(e)}")
            raise
    
    def _get_connection_string(self, db_type, config):
        """
        Get connection string based on database type
        
        Args:
            db_type: Type of database ('main' or 'replica')
            config: Configuration dictionary
            
        Returns:
            PostgreSQL connection string
        """
        conn_string = ""
        if db_type == "main":
            conn_string = config["main_database"]
        elif db_type == "replica":
            conn_string = config["replica_database"]
        else:
            raise ValueError(f"Invalid database type: {db_type}")
            
        # Convert JDBC connection string to psycopg2-compatible format if needed
        if conn_string.startswith("jdbc:postgresql:"):
            conn_string = conn_string.replace("jdbc:postgresql:", "")
        
        return conn_string
    
    def _parse_connection(self, conn_str):
        """
        Parse connection string into host, port, and database components
        
        Args:
            conn_str: Connection string to parse
            
        Returns:
            Tuple of (host, port, database)
        """
        # Default port for PostgreSQL
        port = 5432
        database = "postgres"
        
        # Extract host, port, and database name
        parts = conn_str.split('/')
        if len(parts) > 1:
            hostport = parts[0]
            database = parts[-1]
            
            # Extract host and port
            if ':' in hostport:
                host, port_str = hostport.split(':')
                try:
                    port = int(port_str)
                except ValueError:
                    logger.warning(f"Invalid port in connection string: {port_str}, using default 5432")
            else:
                host = hostport
        else:
            host = conn_str
            logger.warning(f"No database specified in connection string, using default 'postgres'")
        
        return host, port, database
        
    def close(self):
        """Close all connection pools"""
        if self.main_db:
            self.main_db.close()
        if self.replica_db:
            self.replica_db.close()
        self.initialized = False
        logger.info("Shared connection pools closed")

# Create a single shared connection manager instance
shared_connection_manager = SharedConnectionManager()

class PostgreSQLUser(User):
    """
    Locust user class for PostgreSQL load testing
    Simulates user behavior for both primary and replica databases
    """
    
    abstract = True
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.main_db = None
        self.replica_db = None
        
        # Get configuration from environment variables with defaults
        self.config = {
            "main_threads": int(os.getenv("main_threads", DEFAULT_CONFIG["main_threads"])),
            "main_loops": int(os.getenv("main_loops", DEFAULT_CONFIG["main_loops"])),
            "replica_threads": int(os.getenv("replica_threads", DEFAULT_CONFIG["replica_threads"])),
            "replica_loops": int(os.getenv("replica_loops", DEFAULT_CONFIG["replica_loops"])),
            "main_database": os.getenv("main_database", ""),
            "replica_database": os.getenv("replica_database", "")
        }
        
        # Get credentials (should be fetched from Azure Key Vault or environment securely)
        self.credentials = {
            "mainuser": os.getenv("mainuser", ""),
            "mainpassword": os.getenv("mainpassword", ""),
            "replicauser": os.getenv("replicauser", ""),
            "replicapassword": os.getenv("replicapassword", "")
        }

    def get_connection_string(self, db_type: str) -> str:
        """
        Get connection string based on database type
        
        Args:
            db_type: Type of database ('main' or 'replica')
            
        Returns:
            PostgreSQL connection string
        """
        conn_string = ""
        if db_type == "main":
            conn_string = self.config["main_database"]
        elif db_type == "replica":
            conn_string = self.config["replica_database"]
        else:
            raise ValueError(f"Invalid database type: {db_type}")
            
        # Convert JDBC connection string to psycopg2-compatible format if needed
        if conn_string.startswith("jdbc:postgresql:"):
            conn_string = conn_string.replace("jdbc:postgresql:", "")
        
        return conn_string
    
    def on_start(self):
        """Set up database connections on start"""
        try:
            # Initialize shared connection manager if not already initialized
            shared_connection_manager.initialize(self.config, self.credentials)
            
            # Assign shared connection pools to user instance
            self.main_db = shared_connection_manager.main_db
            self.replica_db = shared_connection_manager.replica_db
            
            logger.info("Database connections assigned to user instance")
        except Exception as e:
            logger.error(f"Failed to assign database connections: {str(e)}")
            if hasattr(self, 'environment'):
                self.environment.runner.quit()
            else:
                # If running outside of Locust environment (e.g., in tests)
                raise
    
    def on_stop(self):
        """Clean up resources on stop"""
        logger.info("User instance stopped")

class CombinedDatabaseUser(PostgreSQLUser):
    """
    Combined user class that handles both read and write operations
    to primary and replica databases in the same user session
    
    This unified approach simplifies execution while maintaining the separation
    of read and write workloads that follow Azure best practices.
    """
    
    # Define task weights to maintain the same distribution:
    # - Read operations should be more frequent (similar to ReplicaDatabaseUser)
    # - Write operations less frequent (similar to MainDatabaseUser)
    
    @task(1)  # Weight for write operations
    def write_task(self):
        """Execute write operations against the primary database"""
        if not self.main_db:
            logger.error("Main database connection not available")
            return
        
        start_time = time.time()
        product_names = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
        
        try:
            with self.main_db.get_cursor() as cursor:
                for product in product_names:
                    new_price = random.randint(0, 9999)
                    query = "UPDATE public.products SET price = %s WHERE name = %s"
                    cursor.execute(query, (new_price, product))
                    
                    # Report to Locust statistics
                    request_name = f"UPDATE-{product}"
                    self.environment.events.request.fire(
                        request_type="WRITE",
                        name=request_name,
                        response_time=(time.time() - start_time) * 1000,
                        response_length=0,
                        exception=None,
                    )
            
            logger.debug(f"Updated prices for {len(product_names)} products")
            
            # Add think time to maintain the desired operation rate (similar to original 60 ops/sec)
            time.sleep(random.uniform(0.8, 1.2))
            
        except Exception as e:
            # Log the error and report failure to Locust
            logger.error(f"Failed to update products: {str(e)}")
            self.environment.events.request.fire(
                request_type="WRITE",
                name="UPDATE-Products",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
            )
    
    @task(4)  # Weight for read operations (4x more frequent than writes)
    def read_task(self):
        """Execute read operations against the replica database"""
        if not self.replica_db:
            logger.error("Replica database connection not available")
            return
        
        start_time = time.time()
        try:
            with self.replica_db.get_cursor(commit=False) as cursor:
                query = "SELECT * FROM public.products ORDER BY price DESC"
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Report success to Locust
                self.environment.events.request.fire(
                    request_type="READ",
                    name="SELECT-Products-OrderByPrice",
                    response_time=(time.time() - start_time) * 1000,
                    response_length=len(rows),
                    exception=None,
                )
                
            logger.debug(f"Read {len(rows)} products from replica")
            
            # Add shorter think time for read operations (similar to original 240 ops/sec)
            time.sleep(random.uniform(0.2, 0.5))
            
        except Exception as e:
            # Log the error and report failure to Locust
            logger.error(f"Failed to read products: {str(e)}")
            self.environment.events.request.fire(
                request_type="READ",
                name="SELECT-Products-OrderByPrice",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
            )
    
    # Define a variable wait time between tasks, averaging between the original classes
    wait_time = between(0.5, 2.0)


# Clean up event handling
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize shared connection pools when the test starts"""
    logger.info("PostgreSQL load test is starting")
    
    # Get configuration from environment variables with defaults
    config = {
        "main_threads": int(os.getenv("main_threads", DEFAULT_CONFIG["main_threads"])),
        "main_loops": int(os.getenv("main_loops", DEFAULT_CONFIG["main_loops"])),
        "replica_threads": int(os.getenv("replica_threads", DEFAULT_CONFIG["replica_threads"])),
        "replica_loops": int(os.getenv("replica_loops", DEFAULT_CONFIG["replica_loops"])),
        "main_database": os.getenv("main_database", ""),
        "replica_database": os.getenv("replica_database", "")
    }
    
    # Get credentials
    credentials = {
        "mainuser": os.getenv("mainuser", ""),
        "mainpassword": os.getenv("mainpassword", ""),
        "replicauser": os.getenv("replicauser", ""),
        "replicapassword": os.getenv("replicapassword", "")
    }
    
    try:
        # Initialize shared connection pools
        shared_connection_manager.initialize(config, credentials)
        # Store in environment for users to access if needed
        environment.shared_connection_manager = shared_connection_manager
    except Exception as e:
        logger.error(f"Failed to initialize connection pools: {str(e)}")
        environment.runner.quit()


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Close shared connection pools when the test stops"""
    logger.info("PostgreSQL load test is stopping")
    if hasattr(environment, 'shared_connection_manager'):
        environment.shared_connection_manager.close()
    else:
        # Direct access if not stored in environment
        shared_connection_manager.close()


# Set default user class if running the script directlyss
if __name__ == "__main__":
    # This makes the script use the combined user class by default
    import sys
    if len(sys.argv) == 2 and sys.argv[1].endswith("locust_pg_test.py"):
        sys.argv.append("CombinedDatabaseUser")

# Run the test with following command:
# locust -f locust_pg_test.py CombinedDatabaseUser -u 25 -r 10 -t 5m
# Or start the web UI:
# locust -f locust_pg_test.py
# 
# Note: The script will automatically use CombinedDatabaseUser when started with just:
# locust -f locust_pg_test.py