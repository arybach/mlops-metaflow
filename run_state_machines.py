import boto3
import json

sfn_client = boto3.client('stepfunctions')
# Data prep + embeddings (not needed here, but useful for the next project)
SfUsdaToEsFlow = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfUsdaToEsFlow"
SfSplitDataset = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfSplitDataset"
SfEmbeddingsFlow = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfEmbeddingsFlow"
# Metfalow only
SfEsClustersFlow = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfEsClustersFlow"
SfEsLrFlow =  "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfEsLrFlow"
SfEsXGBoostFlow = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfEsXGBoostFlow"
# + WANDB
SfWandbEsClustersFlow = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfWandbEsClustersFlow"
SfWandbEsLrFlow = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfWandbEsLrFlow"
SfWandbEsXGBoostFlow = "arn:aws:states:ap-southeast-1:388062344663:stateMachine:metaflow-mlops-apse1_SfWandbEsXGBoostFlow"



definition = {
    "Comment": "Metaflow MLOps Chained State Machine",
    "StartAt": "Step1",
    "States": {
        "Step1": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfUsdaToEsFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step2"
        },
        "Step2": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfSplitDataset,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step3"
        },
        "Step3": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfEmbeddingsFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step4"
        },
        "Step4": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfEsClustersFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step5"
        },
        "Step5": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfEsLrFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step6"
        },
        "Step6": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfEsXGBoostFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step7"
        },
        "Step7": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfEsClustersFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step8"
        },
        "Step8": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfEsLrFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "Next": "Step9"
        },
        "Step9": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
                "StateMachineArn": SfEsXGBoostFlow,
                "Input": {
                    "JSON": {
                        "example": "data"
                    }
                }
            },
            "End": True
        }
    }
}

response = sfn_client.create_state_machine(
    name='ChainedUsdaFlow',
    definition=json.dumps(definition),
    roleArn='arn:aws:iam::388062344663:role/metaflow-step_functions_role-mlops-apse1',
)
