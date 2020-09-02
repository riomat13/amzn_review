# this is temporary storage for codes

from settings import Config


# TODO
def redshift_role_creation(config: dict = None) -> Role:
    """IAM Role creation for Redshift.
    
    Args:
        config: dict (default: None)
            must contains following properties
                'IAM_ROLE_NAME',
            if not provided, use variables from `.env` file.

    Returns:
        Role object
            properties:
                client: boto3.client
                role:
                arn: str
    """
    config = config or Config

    iam = AWS.get_client('iam')

    role = iam.create_role(
        Path='/amzn_review_app/',
        RoleName=config['IAM_ROLE_NAME'],
        Description='Allows to access Redshift',
        AssumeRolePolicyDocument=json.dumps({
            'Statement': [
                {
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': [
                            'redshift.amazonaws.com',
                            's3.amazonaws.com'
                        ]
                    }
                }
            ],
            'Version': '2012-10-17'
        })
    )

    iam.attach_role_policy(
            RoleName=config['IAM_ROLE_NAME'],
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess') \
        .get('ResponseMetadata') \
        .get('HTTPStatusCode')

    arn = iam.get_role(RoleName=config['IAM_ROLE_NAME'])['Role']['Arn']

    IAM = _Role(client=iam, role=role, arn=arn)
    return IAM
