#!/bin/bash

function prompt_existing_bucket() {
    # Prompt for name of bucket to output files to
    while true; do
        read -p "Enter the name of an S3 bucket for CloudTrail to output to (must exist already): " TRAIL_BUCKET_NAME

        if [ ! -z "${TRAIL_BUCKET_NAME}" ]; then
            break
        fi
    done
}

function add_cloudtrail_bucket_policy() {
    # https://docs.aws.amazon.com/cli/latest/reference/s3api/put-bucket-policy.html
    TRAIL_BUCKET_NAME=$1
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')

    echo "Creating bucket policy from template.."

    POLICYDOC=$(sed -e "s/\${AWS_ACCOUNT_ID}/${AWS_ACCOUNT_ID}/" -e "s/\${TRAIL_BUCKET_NAME}/${TRAIL_BUCKET_NAME}/" ./scripts/bucket_policy.tpl.json)

    echo "${POLICYDOC}" > ./scripts/bucket_policy.json

    aws s3api put-bucket-policy --bucket "${TRAIL_BUCKET_NAME}" --policy file://scripts/bucket_policy.json > /dev/null 2>&1 && echo "Added bucket policy" || echo "Unable to add bucket policy, please check permissions and try again"
}

function create_new_bucket() {
    # https://docs.aws.amazon.com/cli/latest/reference/s3/mb.html
    while true; do
        read -p "Enter a name for your CloudTrail logs S3 bucket: " TRAIL_BUCKET_NAME

        if [ ! -z "${TRAIL_BUCKET_NAME}" ]; then
            break
        fi
    done

    echo "Creating S3 bucket.."

    aws s3 mb s3://"${TRAIL_BUCKET_NAME}" > /dev/null 2>&1 && echo "Bucket created successfully" && add_cloudtrail_bucket_policy "${TRAIL_BUCKET_NAME}" || echo "Unable to create bucket, please check permissions and try again"
}

function prompt_to_create_bucket() {
    while true; do
        read -p "Would you like to create an S3 bucket to store Cloudtrail logs (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) create_new_bucket; break;;
            No|no) prompt_existing_bucket; break;;
            *) echo "Please answer yes or no";;
        esac
    done
}

function create_cloudtrail() {
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/create-trail.html
    TRAIL_NAME=$1
    TRAIL_REGIONAL_STATUS=$2
    TRAIL_INCLUDE_GLOBAL_EVENTS=$3
    TRAIL_PERFORM_LOG_VALIDATION=$4
    TRAIL_BUCKET_NAME=$5

    echo "Creating trail.."
    aws cloudtrail create-trail \
        --name "${TRAIL_NAME}" \
        --s3-bucket-name "${TRAIL_BUCKET_NAME}" \
        "${TRAIL_REGIONAL_STATUS}" \
        "${TRAIL_INCLUDE_GLOBAL_EVENTS}" \
        "${TRAIL_PERFORM_LOG_VALIDATION}" > /dev/null 2>&1 && echo "Trail created successfully" || echo "Unable to create trail, please check your permissions and try again"
}

function prepare_and_create_cloudtrail() {
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/create-trail.html
    local TRAIL_REGIONAL_STATUS=""
    local TRAIL_INCLUDE_GLOBAL_EVENTS=""
    local TRAIL_PERFORM_LOG_VALIDATION=""

    # Prompt for name of cloudtrail
    read -p "Enter a name for your trail: " TRAIL_NAME

    prompt_to_create_bucket

    # Prompt user if they want it to be a multi region trail (which is recommended)
    while true; do
        read -p "Would you to create multi-region trail (recommended) (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) TRAIL_REGIONAL_STATUS="--is-multi-region-trail"; break;;
            No|no) TRAIL_REGIONAL_STATUS="--no-is-multi-region-trail"; break;;
            *) echo "Please answer yes or no";;
        esac
    done

    if [ ${TRAIL_REGIONAL_STATUS} == "--is-multi-region-trail" ]; then
        TRAIL_INCLUDE_GLOBAL_EVENTS="--include-global-service-events"
    else
        # Prompt user if they want to log global events (IAM)
        while true; do
            read -p "Would you like this trail to log global events (eg: IAM) (yes/no)? " YESNO

            case $YESNO in
                Yes|yes) TRAIL_INCLUDE_GLOBAL_EVENTS="--include-global-service-events"; break;;
                No|no) TRAIL_INCLUDE_GLOBAL_EVENTS="--no-include-global-service-events"; break;;
                *) echo "Please answer yes or no";;
            esac
        done
    fi

    # Prompt user if they want to perform log file validation
    while true; do
        read -p "Would you like to trail to perform log validation (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) TRAIL_PERFORM_LOG_VALIDATION="--enable-log-file-validation"; break;;
            No|no) TRAIL_PERFORM_LOG_VALIDATION="--no-enable-log-file-validation"; break;;
            *) echo "Please answer yes or no";;
        esac
    done

    create_cloudtrail "${TRAIL_NAME}" "${TRAIL_REGIONAL_STATUS}" "${TRAIL_INCLUDE_GLOBAL_EVENTS}" "${TRAIL_PERFORM_LOG_VALIDATION}" "${TRAIL_BUCKET_NAME}"
}

function get_cloudtrail() {
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/get-trail.html
    CTRAIL_NAME=$1

    TRAIL_BUCKET_NAME=$(aws cloudtrail get-trail --name "${CTRAIL_NAME}" | jq -r '.Trail.S3BucketName')
}

function prompt_for_cloudtrail() {
    while true; do
        read -p "Do you already have AWS CloudTrail setup (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) prompt_to_read_cloudtrail_config; break;;
            No|no) prompt_to_create_cloudtrail; break;;
            *) echo "Please answer yes or no";;
        esac
    done
}

function prompt_to_read_cloudtrail_config() {
    while true; do
        read -p "Would you like to automatically configure via a pre-existing trail (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) prompt_for_cloudtrail_name; break;;
            No|no) break;;
            *) echo "Please answer yes or no";;
        esac
    done
}

function prompt_for_cloudtrail_name() {
    while true; do
        read -p "Please enter the name of a pre-existing trail: " CTRAIL_NAME

        if [ ! -z "${CTRAIL_NAME}" ]; then
            get_cloudtrail "${CTRAIL_NAME}"; break
        fi
    done
}

function prompt_to_create_cloudtrail() {
    while true; do
        read -p "Would you like to setup CloudTrail (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) prepare_and_create_cloudtrail; break;;
            No|no) "Passing on Cloudtrail setup.."; break;;
            *) echo "Please answer yes or no";;
        esac
    done
}

function prompt_for_athena() {
    while true; do
        read -p "Would you like to setup a cloudtrail logs table in athena (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) prepare_and_execute_sql_statement "${TRAIL_BUCKET_NAME}"; break;;
            No|no) echo "Passing on Athena setup.."; break;;
            *) echo "Please answer yes or no";;
        esac
    done
}

function create_athena_table() {
    # https://docs.aws.amazon.com/ko_kr/cli/latest/reference/athena/start-query-execution.html
    echo "creating athena table.."
    QUERY_STRING=$1
    TABLE_DATABASE=$2
    TABLE_DATA_CATALOG=$3
    QUERY_OUTPUT_LOC=$4

    aws athena start-query-execution \
        --query-string "${QUERY_STRING}" \
        --query-execution-context Database="${TABLE_DATABASE}",Catalog="${TABLE_DATA_CATALOG}" \
        --result-configuration OutputLocation="s3://${QUERY_OUTPUT_LOC}" > /dev/null 2>&1 && echo "Table created successfully" || echo "Unable to execute Athena query, please try again"
}

function prepare_and_execute_sql_statement() {
    TRAIL_BUCKET_NAME=$1
    # Retrieve the account id which helps determine where the logs will be located
    echo "Retrieving account configuration.."
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')

    read -p "Enter the name of the database you'd like to create the table in (default: default): " TABLE_DATABASE
    read -p "Enter the name of the data catalog the database is in (default: AwsDataCatalog): " TABLE_DATA_CATALOG

    while true; do
        read -p "Enter the name of your cloudtrail table: " TABLE_NAME

        if [ ! -z "${TABLE_NAME}" ]; then
            # Athena tables cannot contain dashes
            if [[ "${TABLE_NAME}" =~ ^[A-Za-z0-9_]*$ ]]; then
                break
            else
                echo "Table names must only contain letters, numbers, and underscores"
            fi
        else
            echo "Table name must be set"
        fi
    done

    if [ -z "${TABLE_DATABASE}" ]; then
        TABLE_DATABASE="default"
    fi

    if [ -z "${TABLE_DATA_CATALOG}" ]; then
        TABLE_DATA_CATALOG="AwsDataCatalog"
    fi

    QUERY_STRING=$(sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/ /g' -e "s/\${ATHENA_TABLE_NAME}/${TABLE_NAME}/" -e "s/\${ACCOUNT_ID}/${AWS_ACCOUNT_ID}/" -e "s/\${TRAIL_BUCKET_NAME}/${TRAIL_BUCKET_NAME}/" ./scripts/create_table.tpl.sql)

    while true; do
        read -p "Enter the name of the bucket and optionally a prefix for the output of the create table query (path optional): " QUERY_OUTPUT_LOC

        if [ ! -z "${QUERY_OUTPUT_LOC}" ]; then
            break
        else
            echo "Table name must be set"
        fi
    done

    create_athena_table "${QUERY_STRING}" "${TABLE_DATABASE}" "${TABLE_DATA_CATALOG}" "${QUERY_OUTPUT_LOC}"
}

function zip_lambda() {
    # Clean up old
    rm -f function.zip

    # Zip new!
    zip -j function.zip reloader/main.py
}

function prompt_for_lambda() {
    while true; do
        read -p "Would you like to setup the lambda (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) prepare_and_create_lambda; break;;
            No|no) "Passing on lambda and event rule setup.."; break;;
            *) echo "Please answer yes or no";;
        esac
    done
}

function create_lambda() {
    # https://docs.aws.amazon.com/cli/latest/reference/lambda/create-function.html
    LAMBDA_NAME=$1
    LAMBDA_ROLE_ARN=$2

    # Zip the code
    zip_lambda

    if [ -z "${AWS_ACCOUNT_ID}" ]; then
        AWS_ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')
    fi

    if [ -z "${TABLE_DATABASE}" ]; then
        while true; do
            read -p "Enter the name of the database where the table is located: " TABLE_DATABASE

            if [ ! -z "${TABLE_DATABASE}" ]; then
                break
            fi
        done
    fi

    if [ -z "${TABLE_NAME}" ]; then
        while true; do
            read -p "Enter the name of the name of the table where logs are stored: " TABLE_NAME

            if [ ! -z "${TABLE_NAME}" ]; then
                break
            fi
        done
    fi

    aws lambda create-function \
        --function-name "${LAMBDA_NAME}" \
        --runtime python3.8 \
        --zip-file fileb://function.zip \
        --handler main.lambda_handler \
        --environment "Variables={BUCKET=${TRAIL_BUCKET_NAME},ACCOUNT_ID=${AWS_ACCOUNT_ID},DATABASE=${TABLE_DATABASE},LOG_LOCATION=AWSLogs,TABLE_NAME=${TABLE_NAME},OUTPUT_LOC=s3://${QUERY_OUTPUT_LOC}}" \
        --role "${LAMBDA_ROLE_ARN}" > /dev/null 2>&1 && echo "Lambda created successfully" || "Unable to create lambda, please check your configuration and try again"
}

function create_lambda_role() {
    # https://docs.aws.amazon.com/cli/latest/reference/iam/create-role.html
    read -p "Enter a name for the lambda execution role (default: ReloaderRole): " ROLENAME

    if [ -z "${ROLENAME}" ]; then
        ROLENAME="ReloaderRole"
    fi

    echo "Creating role.."

    LAMBDA_ROLE_ARN=$(aws iam create-role \
        --path "/service-role/" \
        --role-name "${ROLENAME}" \
        --assume-role-policy-document file://scripts/assume_role_policy_doc.json | jq -r '.Role.Arn')

    echo "Role created successfully"

    # Kind of a hack but
    # w/o sleep lambda will thow an InvalidParameterValueException
    # stating that "The role defined for the function cannot be assumed by Lambda."
    echo "Sleeping to allow role to propagate.."
    sleep 5
}

function create_policy_doc() {
    # https://docs.aws.amazon.com/cli/latest/reference/iam/create-policy.html
    AWS_REGION=$(aws configure get region)

    if [ -z "${QUERY_OUTPUT_LOC}" ]; then
        read -p "Enter the name of the bucket where query outputs will be stored: " QUERY_OUTPUT_LOC
    fi

    POLICYDOC=$(sed -e "s/\${AWS_REGION}/${AWS_REGION}/" -e "s/\${AWS_ACCOUNT_ID}/${AWS_ACCOUNT_ID}/" -e "s/\${QUERY_OUTPUT_LOC}/${QUERY_OUTPUT_LOC}/" -e "s/\${LAMBDA_NAME}/${LAMBDA_NAME}/" -e "s/\${TRAIL_BUCKET_NAME}/${TRAIL_BUCKET_NAME}/" ./scripts/iam_policy.tpl.json)

    echo "${POLICYDOC}" > ./scripts/iam_policy.json
}

function attach_lambda_policy_to_role() {
    # https://docs.aws.amazon.com/cli/latest/reference/iam/attach-role-policy.html
    POLICYARN=$1

    aws iam attach-role-policy \
        --role-name "${ROLENAME}" \
        --policy-arn "${POLICYARN}" > /dev/null 2>&1 && echo "Successfully attached policy to role" || echo "Unable to attach policy to role please check permissions and try again"
}

function create_lambda_policy() {
    read -p "Enter a name for the lambda policy (default: ReloaderPolicy): " POLICYNAME

    if [ -z "${POLICYNAME}" ]; then
        POLICYNAME="ReloaderPolicy"
    fi

    echo "Creating IAM policy.."
    create_policy_doc

    POLICYARN=$(aws iam create-policy \
        --policy-name "${POLICYNAME}" \
        --policy-document file://scripts/iam_policy.json | jq -r '.Policy.Arn')

    attach_lambda_policy_to_role "${POLICYARN}"
}

function prepare_and_create_lambda() {
    # https://docs.aws.amazon.com/cli/latest/reference/lambda/create-function.html
    while true; do
        read -p "Please enter a name for your lambda: " LAMBDA_NAME

        if [ ! -z "${LAMBDA_NAME}" ]; then
            break
        fi
    done

    create_lambda_role
    create_lambda_policy

    create_lambda "${LAMBDA_NAME}" "${LAMBDA_ROLE_ARN}"
}

function add_lambda_policy() {
    # https://docs.aws.amazon.com/cli/latest/reference/lambda/add-permission.html
    EVENT_SOURCE_ARN=$1

    echo "Adding event permission to trigger lambda.."

    aws lambda add-permission \
        --function-name "${LAMBDA_NAME}" \
        --action lambda:InvokeFunction \
        --statement-id EventTriggerLambda \
        --principal events.amazonaws.com \
        --source-arn "${EVENT_SOURCE_ARN}" > /dev/null 2>&1 && echo "Lambda policy created successfully" || echo "Unable add event invoke permissions to lambda"
}

function add_event_target() {
    # https://docs.aws.amazon.com/cli/latest/reference/events/put-targets.html
    echo "Adding event target.."

    FUNCTION_ARN=$(aws lambda get-function --function-name "${LAMBDA_NAME}" | jq -r '.Configuration.FunctionArn')

    aws events put-targets \
        --rule "EveryNightAtMidnight-Reloader" \
        --targets "Id"="LambdaTarget","Arn"="${FUNCTION_ARN}" > /dev/null 2>&1 && echo "Rule target added successfully" || echo "Unable to add rule target, please check permissions and try again"
}

function add_event_rule() {
    # https://docs.aws.amazon.com/cli/latest/reference/events/put-rule.html
    echo "Creating Event rule to trigger every night at midnight.."

    EVENT_SOURCE_ARN=$(aws events put-rule \
        --name "EveryNightAtMidnight-Reloader" \
        --schedule-expression "cron(0 0 * * ? *)" \
        --state ENABLED | jq -r '.RuleArn')

    echo "Event created successfully"

    add_lambda_policy "${EVENT_SOURCE_ARN}"
    add_event_target
}

prompt_for_cloudtrail
prompt_for_athena
prompt_for_lambda
add_event_rule
