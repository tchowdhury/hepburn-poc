from constructs import Construct
import os
import json
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_stepfunctions_tasks as sfn_tasks
import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions_tasks as sfn_tasks
from aws_cdk.aws_stepfunctions import Timeout
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration, Aws)
import amazon_textract_idp_cdk_constructs as tcdk
import aws_cdk.aws_apigatewayv2 as apigwv2
import aws_cdk.aws_apigatewayv2_integrations as apigwv2_integrations
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

class HepburnStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id,
                         description=
                         "Hepburn AP workflow Stack for AWS CDK",
                           **kwargs)

        
        # Read AWS account and region from config.json
            
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)

        account = config.get('aws_account_id')
        client_name = config.get('client_name', 'Hepburn')
        workflow_name = config.get('workflow_name', 'Hepburn-APProcessingWorkflow')
        adapter_id = config.get('adapter_id')
        version = config.get('version')
        profile = "laddprofile"
        session = boto3.Session(profile_name=profile)
        region = session.region_name
        parameter_name = os.getenv('PARAMETER_NAME')

        script_location = os.path.dirname(__file__)

        bucket_name=f"{account}-{client_name}".lower()
        s3_upload_prefix = "upload"

        # Document bucket
        document_bucket = s3.Bucket(self,
            f"IDP{client_name}Documents",
            bucket_name=bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=False,
            versioned=True,
            minimum_tls_version=1.2,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            cors=[
                s3.CorsRule(
                    allowed_headers=["*"],
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.HEAD
                    ],
                    allowed_origins=["*"]
                )
            ], 
            enforce_ssl=True)
        
        s3_output_bucket = bucket_name

        # Build parameter ARN for SecureString parameter
        jwt_secret_param_arn = f"arn:aws:ssm:{region}:{account}:parameter{parameter_name}"

        # JWT Lambda Layer
        jwt_layer = lambda_.LayerVersion(self, "JWTLayer",
            code=lambda_.Code.from_asset("_lambda/layer/jwt_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="JWT library layer for authentication"
        )


        # Lambda: authenticate and getPresignedUrl
        auth_get_presigned_url = lambda_.Function(self, "GeneratePresignedUrl",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="auth_generate_presigned_url.handler",
            code=lambda_.Code.from_asset("_lambda/auth_generate_presigned_url"),
            environment={
                "BUCKET_NAME": s3_output_bucket,
                "UPLOAD_PREFIX": s3_upload_prefix,
                "PARAMETER_NAME": parameter_name
            },
            timeout=Duration.seconds(300),
            memory_size=128,
            layers=[jwt_layer]
        )
        

        document_bucket.grant_put(auth_get_presigned_url)

        # Add SSM GetParameter permission for JWT secret
        auth_get_presigned_url.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=[jwt_secret_param_arn]
            )
        )

        # HTTP API Gateway
        http_api = apigwv2.HttpApi(self, "DocumentUploadHttpApi",
            api_name="Document Upload HTTP API",
            create_default_stage=False,
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_headers=["Authorization"],
                allow_methods=[apigwv2.CorsHttpMethod.GET],
                allow_origins=["*"]
            )
        )
        
        # Create prod stage
        prod_stage = apigwv2.HttpStage(self, "ProdStage",
            http_api=http_api,
            stage_name="prod",
            auto_deploy=True
        )

        # Integrations
        validate_integration = apigwv2_integrations.HttpLambdaIntegration(
            "GeneratePresignedUrlIntegration",
            auth_get_presigned_url
        )

        # Route
        http_api.add_routes(
            path="/preSignedUrl",
            methods=[apigwv2.HttpMethod.GET],
            integration=validate_integration
        )

        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        # Lambda function to move the file from upload to landing
        lambda_move_file_to_landing_function = lambda_.DockerImageFunction(
            self,
            "LambdaMovetoLanding",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../_lambda/movetolandingfunction')),
            memory_size=128,
            timeout=Duration.seconds(300),
            architecture=lambda_.Architecture.X86_64)

        
        lambda_move_file_to_landing_task = sfn_tasks.LambdaInvoke(
            self,
            "MoveFileToLandingTask",
            lambda_function=lambda_move_file_to_landing_function
        )


        # Lambda function to classify the document
        lambda_classify_document_function = lambda_.DockerImageFunction(
            self,
            "LambdaClassifyDocument",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../_lambda/classifydocumentfunction')),
            memory_size=128,
            timeout=Duration.seconds(300),
            architecture=lambda_.Architecture.X86_64)
        


        # Lambda task to classify the document
        lambda_classify_document_task = sfn_tasks.LambdaInvoke(
            self,
            "ClassifyDocumentTask",
            lambda_function=lambda_classify_document_function
        )

        # Lambda function to archive the original file to archive location
        lambda_archive_document_function = lambda_.DockerImageFunction(
            self,
            "LambdaArchiveDocument",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../_lambda/archivedocumentfunction')),
            memory_size=128,
            timeout=Duration.seconds(300),
            architecture=lambda_.Architecture.X86_64)

        # Add S3 permissions for archive lambda
        lambda_archive_document_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                resources=[
                    f"arn:aws:s3:::{document_bucket.bucket_name}/*"
                ]
            )
        )

        # Lambda task to archive the document
        lambda_archive_document_task = sfn_tasks.LambdaInvoke(
            self,
            "ArchiveDocumentTask",
            lambda_function=lambda_archive_document_function
        )

        # Lambda function to derive queries dynamically either default or custom based on document type
        lambda_dynamic_queries_function = lambda_.DockerImageFunction(
            self,
            "LambdaDynamicQueries",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../_lambda/dynamicqueriesfunction')),
            memory_size=128,
            timeout=Duration.seconds(300),
            architecture=lambda_.Architecture.X86_64)

        # Lambda task to archive the document
        lambda_dynamic_query_task = sfn_tasks.LambdaInvoke(
            self,
            "DynamicQueryTask",
            lambda_function=lambda_dynamic_queries_function
        )

        # Add S3 permissions for dynamic queries lambda
        lambda_dynamic_queries_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                resources=[
                    f"arn:aws:s3:::{document_bucket.bucket_name}/*"
                ]
            )
        )

        
        # Add s3:ListBucket permission for dynamic queries lambda
        lambda_dynamic_queries_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[f"arn:aws:s3:::{document_bucket.bucket_name}"]
            )
        )

        

        # Lambda function to extract data async using Textract
        lambda_async_textract_function = lambda_.DockerImageFunction(
            self,
            "LambdaAsyncTextract",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../_lambda/textractfunction')),
            memory_size=128,
            timeout=Duration.seconds(600),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "ADAPTER_ID": adapter_id,
                "VERSION": version
            }
        )

        lambda_async_textract_function.add_to_role_policy(
        iam.PolicyStatement(
            actions=["textract:StartDocumentAnalysis"],
            resources=[f"arn:aws:textract:ap-southeast-2:291792795916:/adapters/{adapter_id}/versions/{version}"]
            )
        )

        lambda_async_textract_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["textract:GetDocumentAnalysis"],
                resources=["*"]
            )
        )

        # Lambda task to archive the document
        lambda_async_textract_task = sfn_tasks.LambdaInvoke(
            self,
            "AsyncTextractTask",
            lambda_function=lambda_async_textract_function
        )

        # Add S3 permissions for dynamic queries lambda
        lambda_async_textract_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                resources=[
                    f"arn:aws:s3:::{document_bucket.bucket_name}/*"
                ]
            )
        )

        #-------------


        # # Add Pass state to wrap Payload in manifest for TextractAsync
        # transform_for_textract = sfn.Pass(
        #     self,
        #     "TransformForTextract",
        #     parameters={
        #         "manifest": sfn.JsonPath.string_at("$.Payload")
        #     }
        # )

        # textract_async_task = tcdk.TextractGenericAsyncSfnTask(
        #     self,
        #     "TextractAsync",
        #     s3_output_bucket=s3_output_bucket,
        #     s3_temp_output_prefix="raw/textract-temp-output",
        #     integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
        #     lambda_log_level="DEBUG",
        #     task_timeout=Timeout.duration(Duration.hours(24)),
        #     input=sfn.TaskInput.from_object({
        #         "Token": sfn.JsonPath.task_token,
        #         "ExecutionId": sfn.JsonPath.string_at('$$.Execution.Id'),
        #         "manifest": {
        #             "s3Path": sfn.JsonPath.string_at("$.manifest.s3_path"),
        #             "textractFeatures": ["QUERIES", "TABLES"],
        #             "textractQueries": sfn.JsonPath.string_at("$.manifest.query"),
        #             "adapterconfig": {"adapter_id": adapter_id, "version": version}
        #         }
                
        #     }),
        #     result_path="$.textract_result")

        # # Transform data structure for TextractAsyncToJSON
        # transform_for_json = sfn.Pass(
        #     self,
        #     "TransformForJSON",
        #     parameters={
        #         "manifest": {
        #             "s3Path": sfn.JsonPath.string_at("$.Payload.s3_path"),
        #                 "textractFeatures": ["QUERIES", "TABLES"],
        #                 "textractQueries": sfn.JsonPath.string_at("$.Payload.query"),
        #                 "adapterconfig": {"adapter_id": adapter_id, "version": version},
        #                 "source_bucket": sfn.JsonPath.string_at("$.Payload.source_bucket"),
        #                 "source_key": sfn.JsonPath.string_at("$.Payload.source_key"),
        #                 "mime_type": sfn.JsonPath.string_at("$.Payload.mime_type"),
        #                 "new_s3_key": sfn.JsonPath.string_at("$.Payload.new_s3_key"),
        #                 "s3_bucket": sfn.JsonPath.string_at("$.Payload.s3_bucket"),
        #                 "new_file_name": sfn.JsonPath.string_at("$.Payload.newFileName"),
        #                 "classification": sfn.JsonPath.string_at("$.Payload.classification")
        #         },
        #         "textract_result": sfn.JsonPath.object_at("$.textract_result")
        #     }
        # )

        # textract_async_to_json = tcdk.TextractAsyncToJSON(
        #     self,
        #     "AsyncToJSON",
        #     s3_output_prefix= "raw/textract-json",
        #     s3_output_bucket=s3_output_bucket,
        #     lambda_memory_mb=3008)

        # # Grant S3 permissions to the underlying Lambda functions in the Textract tasks
        # for task in [textract_async_task, textract_async_to_json]:
        #     for node in task.node.children:
        #         if hasattr(node, 'role') and node.role:
        #             node.add_to_role_policy(
        #                 iam.PolicyStatement(
        #                     effect=iam.Effect.ALLOW,
        #                     actions=[
        #                         "s3:GetObject",
        #                         "s3:PutObject",
        #                         "s3:DeleteObject",
        #                         "s3:GetObjectVersion",
        #                         "s3:ListBucket"
        #                     ],
        #                     resources=[
        #                         document_bucket.bucket_arn,
        #                         f"{document_bucket.bucket_arn}/*"
        #                     ]
        #                 )
        #             )
        
        # async_chain = sfn.Chain.start(textract_async_task).next(
        #     transform_for_json).next(textract_async_to_json)
        
        #-------------

        workflow_chain = sfn.Chain \
            .start(decider_task) \
            .next(lambda_classify_document_task) \
            .next(lambda_move_file_to_landing_task) \
            .next(lambda_dynamic_query_task) \
            .next(lambda_async_textract_task) 

        state_machine = sfn.StateMachine(self,
                                         workflow_name,
                                         definition_body=sfn.DefinitionBody.from_chainable(workflow_chain))

        # Lambda function to start the Step Function - create without state machine ARN first
        lambda_step_start_step_function = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionGeneric",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../_lambda/startstepfunction')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64)

        # Add state machine ARN to environment after both resources exist
        lambda_step_start_step_function.add_environment("STATE_MACHINE_ARN", state_machine.state_machine_arn)

        # Grant permissions
        document_bucket.grant_read_write(lambda_move_file_to_landing_function)
        #document_bucket.grant_read_write(lambda_adjust_raw_output)
        document_bucket.grant_read(lambda_classify_document_function)
        state_machine.grant_start_execution(lambda_step_start_step_function)

        # Add S3 event notification
        document_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_step_start_step_function),
            s3.NotificationKeyFilter(prefix=s3_upload_prefix))


        # OUTPUT
        CfnOutput(self, "AWSAccountID", value=account)
        CfnOutput(self, "AWSRegion", value=region)
        CfnOutput(self, "ClientName", value=client_name)
        CfnOutput(self, "DocumentBucketName", value=document_bucket.bucket_name)
        CfnOutput(self, "DocumentUploadLocation", value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/")
        CfnOutput(self, "JWTLayerArn", value=jwt_layer.layer_version_arn, export_name="HepburnJWTLayerArn")
        CfnOutput(self, "HttpApiEndpointUrl", value=f"{http_api.api_endpoint}/prod", description="HTTP API endpoint URL")
        CfnOutput(self, "PresignURLArn", value=auth_get_presigned_url.function_arn, export_name="HepburnPresignURLArn")
        CfnOutput(
            self,
            f"{workflow_name}-StartStepFunctionLambdaLogGroup",
            value=lambda_step_start_step_function.log_group.log_group_name)
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            f"{workflow_name}-StepFunctionFlowLink",
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}")
        CfnOutput(self,
            f"{workflow_name}-moveFileToLandingLambdaGroup",
            value=lambda_move_file_to_landing_function.log_group.log_group_name)