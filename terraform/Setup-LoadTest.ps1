# Azure Load Test Setup Script
# This script configures a load test in Azure using the variables from load_test_variables.ps1

# Path to the variables file
$VariableScript = "./Set-LoadTestEnv.ps1" 
$VariablesFile = "./load_test_variables.env"

# Check if the variables file exists
if (-Not (Test-Path -Path $VariablesFile)) {
    Write-Host "`n❌ Error: Variables file not found at $VariablesFile"
    Write-Host "Make sure you run this script from the directory containing load_test_variables.ps1"
    exit 1
}

# Source the variables and verify they are loaded
Write-Host "`n💾 Loading configuration from $VariablesFile..."
. $VariableScript

# Check if the Azure CLI load testing extension is installed
Write-Host "`n🔍 Checking for Azure CLI load testing extension..."
$ExtensionInstalled = az extension list | ConvertFrom-Json | Where-Object { $_.name -eq "load-testing" }
if (-Not $ExtensionInstalled) {
    Write-Host "Installing Azure CLI load testing extension..."
    az extension add -n load
}

# Get current subscription ID
$SubscriptionId = az account show --query id -o tsv
Write-Host "`n📜 Using subscription: $SubscriptionId"

# Create or update the test
Write-Host "`n🛠️ Creating and configuring the load test..."

# Step 1: Check if the test already exists and create it if not
$TestExists = az load test list `
    --resource-group $RESOURCE_GROUP `
    --load-test-resource $LOAD_TEST_NAME `
    -o tsv 2>$null

if (-Not $TestExists) {
    Write-Host "`n💫 Creating new test '$TEST_NAME'..."

    # Create a test with basic parameters
    az load test create `
        --test-id $TEST_NAME `
        --display-name $TEST_NAME `
        --description $TEST_DESCRIPTION `
        --resource-group $RESOURCE_GROUP `
        --load-test-resource $LOAD_TEST_NAME `
        --autostop-error-rate $ERROR_PERCENTAGE `
        --autostop-time-window $TIME_WINDOW `
        --autostop enable `
        --test-type "Locust" `
        --test-plan "../load_test_artifacts/locust_pg_test.py" `
        --engine-instances $ENGINE_INSTANCES `
        --engine-ref-id-type SystemAssigned `
        --keyvault-reference-id $USER_ASSIGNED_IDENTITY_RESOURCE_NAME `
        --metrics-reference-id $USER_ASSIGNED_IDENTITY_RESOURCE_NAME `
        --env main_threads=$MAIN_THREADS main_loops=$MAIN_LOOPS main_database=$MAIN_DATABASE replica_threads=$REPLICA_THREADS replica_loops=$REPLICA_LOOPS replica_database=$REPLICA_DATABASE main_writes_per_minute=$MAIN_WRITES_PER_MINUTE replica_reads_per_minute=$REPLICA_READS_PER_MINUTE `
        --secret mainpassword=$POSTGRES_ADMIN_PASSWORD_SECRET replicapassword=$POSTGRES_ADMIN_PASSWORD_SECRET mainuser=$POSTGRES_ADMIN_USERNAME_SECRET replicauser=$POSTGRES_ADMIN_USERNAME_SECRET
} else {
    Write-Host "`n👷 Test '$TEST_NAME' already exists, updating configuration..."
    
    # Update test with basic parameters
    az load test update `
        --test-id $TEST_NAME `
        --display-name $TEST_NAME `
        --description $TEST_DESCRIPTION `
        --resource-group $RESOURCE_GROUP `
        --load-test-resource $LOAD_TEST_NAME `
        --autostop-error-rate $ERROR_PERCENTAGE `
        --autostop-time-window $TIME_WINDOW `
        --autostop enable `
        --test-plan $SCRIPT_PATH `
        --engine-instances $ENGINE_INSTANCES `
        --engine-ref-id-type SystemAssigned `
        --keyvault-reference-id $USER_ASSIGNED_IDENTITY_RESOURCE_NAME `
        --metrics-reference-id $USER_ASSIGNED_IDENTITY_RESOURCE_NAME `
        --env main_threads=$MAIN_THREADS main_loops=$MAIN_LOOPS main_database=$MAIN_DATABASE replica_threads=$REPLICA_THREADS replica_loops=$REPLICA_LOOPS replica_database=$REPLICA_DATABASE main_writes_per_minute=$MAIN_WRITES_PER_MINUTE replica_reads_per_minute=$REPLICA_READS_PER_MINUTE `
        --secret mainpassword=$POSTGRES_ADMIN_PASSWORD_SECRET replicapassword=$POSTGRES_ADMIN_PASSWORD_SECRET mainuser=$POSTGRES_ADMIN_USERNAME_SECRET replicauser=$POSTGRES_ADMIN_USERNAME_SECRET
}

# Step 2: Upload additional files - this often requires manual steps
if (Test-Path -Path $JAR_PATH) {
    Write-Host "`n✅ JDBC driver found at: $JAR_PATH"
    Write-Host "`nPlease upload this file manually through the Azure Portal."
    Write-Host "`nTo upload the JDBC driver manually:"
    Write-Host "`n1. Go to Azure Portal > Load Testing > $LOAD_TEST_NAME > Tests"
    Write-Host "2. Select the test '$TEST_NAME'"
    Write-Host "3. Click 'Configure', select 'Test' and navigate to the 'Test Plan' section"
    Write-Host "4. Upload the JDBC driver file: $JAR_PATH"
} else {
    Write-Host "`n❌ Warning: JDBC driver not found at $JAR_PATH"
}

Write-Host "`n‼️ Please manually update the following settings in the load test:"
Write-Host "`n1. Go to Azure Portal > Load Testing > $LOAD_TEST_NAME > Tests"
Write-Host "2. Select the test '$TEST_NAME'"
Write-Host "3. Click 'Configure', select 'Test'."
Write-Host "4. In the 'Test Plan' section, set 'Identity' to 'User-assigned identity' and select '$USER_ASSIGNED_IDENTITY_NAME'."
Write-Host "5. In the 'Parameters' section, set the 'Key Vault reference identity'to 'User-assigned identity' and select '$USER_ASSIGNED_IDENTITY_NAME'."
Write-Host "6. In the 'Monitoring' section, under 'Resources', add the two PostgreSQL databases: '$PRIMARY_SERVER_NAME' and '$REPLICA_SERVER_NAME'."
Write-Host "7. In the 'Monitoring' section, under 'Metrics reference identity', set the 'Identity type' to 'User-assigned identity' and select '$USER_ASSIGNED_IDENTITY_NAME'."
Write-Host "8. Click 'Apply' to save the changes."
Write-Host "`n✅ Load test setup script completed."
