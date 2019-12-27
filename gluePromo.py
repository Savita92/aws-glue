#2018 Boto3 script modified for performance and utilizing new learnings


import boto3, pprint, time, awsKeys, Config
#List Glue Jobs & Copy to other ENV
#Pass Glue Job name variable to Promote in Array
PromotionNames = ['GLueJob1, GlueJob2']

#To-do Read Glue Job Names from text file, line-by-line and fire off promotion

def boto3_read_job(usrI):
    session = boto3.Session(profile_name='NonProd')
    client = session.client('glue')
    try:
        glueJson = client.get_job(JobName='AAH_ETL_Feeder_Control')
        print("Job Details Found....")
        return glueJson['Job']
    except client.exceptions.EntityNotFoundException:
        print("Glue Job Not Found" + "jobName")


def S3envpathreplace(glueJson):
    glueJson.pop('CreatedOn')
    glueJson.pop('LastModifiedOn')
    #glueJson.pop('ResponseMetadata')
    glueJson.pop('AllocatedCapacity')
    glueJson['Command']['ScriptLocation'] = glueJson['Command']['ScriptLocation'].replace('dev', 'uat')
    glueJson['Name'] = glueJson['Name'].replace('AAH_ETL_Feeder_Control', 'JAZSUPERAWESOMEGLUE')
    for key, value in glueJson['DefaultArguments'].items():
        glueJson['DefaultArguments'][key] = glueJson['DefaultArguments'][key].replace('dev', 'uat')
    return glueJson


def arn_strip(glueJson):
    glueJson['Role'] = glueJson['Role'] .split('/', 1)[1]
    return glueJson


def boto3_deploy_job(**kwargs):
    print("Creating New Job....")
    sessionPrd = boto3.Session(profile_name='Prod')
    client = sessionNonPrd.client('glue')
    client.create_job(**kwargs)
    print("New Job Created....")


def boto_stuff(**kwargs): return {key: kwargs[key] for key in Config.params if key in kwargs.keys()}


if __name__ == '__main__':
    #Temporary input i provide ===  AAH_ETL_Feeder_Control
    usrI = input("JobName:")
    readJob = boto3_read_job(usrI)
    s3Rep = S3envpathreplace(readJob)
    arnReplace = arn_strip(s3Rep)
    gluePromo = boto_stuff(**arnReplace)
    boto3_deploy_job(**gluePromo)
