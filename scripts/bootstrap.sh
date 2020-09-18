#!/bin/bash

function create_cloudtrail() {
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/create-trail.html
    TRAIL_NAME=$1
    TRAIL_REGIONAL_STATUS=$2
    TRAIL_INCLUDE_GLOBAL_EVENTS=$3
    TRAIL_PERFORM_LOG_VALIDATION=$4
    TRAIL_BUCKET_NAME=$5

    echo "Creating trail.."
    aws cloudtrail create-trail --name "${TRAIL_NAME}" --s3-bucket-name "${TRAIL_BUCKET_NAME}" "${TRAIL_REGIONAL_STATUS}" "${TRAIL_INCLUDE_GLOBAL_EVENTS}" "${TRAIL_PERFORM_LOG_VALIDATION}" > /dev/null 2>&1 && echo "Trail created successfully" || echo "Unable to create trail, please check your permissions and try again"
}

function prepare_and_create_cloudtrail() {
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/create-trail.html
    local TRAIL_REGIONAL_STATUS=""
    local TRAIL_INCLUDE_GLOBAL_EVENTS=""
    local TRAIL_PERFORM_LOG_VALIDATION=""

    # Prompt for name of cloudtrail
    read -p "Enter a name for your trail: " TRAIL_NAME

    # Prompt for name of bucket to output files to
    read -p "Enter the name of an S3 bucket for CloudTrail to output to (must exist already): " TRAIL_BUCKET_NAME

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

function prompt_for_cloudtrail() {
    while true; do
        read -p "Do you already have AWS CloudTrail setup (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) echo "Cloudtrail setup, passing"; break;;
            No|no) prompt_to_create_cloudtrail; break;;
            *) echo "Please answer yes or no";;
        esac
    done
}

function prompt_to_create_cloudtrail() {
    while true; do
        read -p "Would you like to setup cloudtrail (yes/no)? " YESNO

        case $YESNO in
            Yes|yes) prepare_and_create_cloudtrail; break;;
            No|no) "Passing on Cloudtrail setup.."; break;;
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

    aws athena start-query-execution --query-string "${QUERY_STRING}" --query-execution-context Database="${TABLE_DATABASE}",Catalog="${TABLE_DATA_CATALOG}" --result-configuration OutputLocation="s3://${QUERY_OUTPUT_LOC}" > /dev/null 2>&1 && echo "Table created" || echo "Unable to execute Athena query, please try again"
}

function prepare_and_execute_sql_statement() {
    TRAIL_BUCKET_NAME=$1
    # Retrieve the account id which helps determine where the logs will be located
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity | jq -r '.Account')

    read -p "Enter the name of the database you'd like to create the table in (default: default): " TABLE_DATABASE
    read -p "Enter the name of the data catalog the database is in (default: AwsDataCatalog): " TABLE_DATA_CATALOG

    while true; do
        read -p "Enter the name of your cloudtrail table: " TABLE_NAME

        if [ ! -z "${TABLE_NAME}" ]; then
            break
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
