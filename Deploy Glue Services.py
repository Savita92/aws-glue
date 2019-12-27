# Create New Glue Jobs from TextFile Template
#Assumes Text File has no "" characters (i.e. Name = SLFGlueJob#1)

#import Boto3 AWS APIs, pprint (Beautify Jsons files), defaultdict for creating Dict of TextFile
import boto3, pprint
#import awsKeys  #Authentication to Create keys for all environments (Prod, NonProd, Lab, Stage)
from collections import defaultdict
#assign Glue Api to variable 'Client'
#client = boto3.client('glue')
global awsEnv


def input_glue_env(env_input):
    #   while True:
    # if not env_input.isdigit() or env_input > 4 or env_input < 1:
    #     print("Please select the correct environment above")
    #     continue
    # else:

    for k, v in awsEnvPath.items():
        if env_input == k:
            sysEnv = v
    return sysEnv


def input_glue_service(serv_input):
    for k, v in glueService.items():
        if serv_input == k:
            service = v
    return service


def check_job_langauge(fPath):
    if fPath.find('python'):
        jobLang = 'python'
    elif fPath.find('scala'):
        jobLang = 'scala'
    return jobLang


def input_glue_process(deploy_input):
    for k, v in glueProcess.items():
        if serv_input == k:
            glueP = v
    return glueP


def promotion_glue(fPath):
    # read string do stuff
    return
awsEnvPath = {
    '1': 'Prod',
    '2': 'Stage',
    '3': 'NonProd',
    '4': 'Lab',
}
glueService = {
    '1': 'Connection',
    '2': 'Crawler',
    '3': 'Job',
}
glueProcess = {
    '1': 'Create',
    '2': 'Promotion',
}
pythonJobDict = {
    'Name': '',
    'Description': '',
    'Role': '',
    'ScriptLocation': '',
    'TempDir': '',
    'PythonLibraryPath': '',
    'PyFiles': '',
    'envFilePath': '',
    'cfgFilePath': '',
    'WorkerType': '',
    'NumberOfWorkers': '',
}

scalaJobDict = {
    'Name': '',
    'Description': '',
    'Role': '',
    'ScriptLocation': '',
    'TempDir': '',
    'DependentJarsPath': '',
    'ScalaClassName':'',
    'WorkerType': '',
    'NumberOfWorkers': '',
    'GlueVersion': '',
    'Timeout': '',
    'MaxCapacity': '',
}

connectionDict = {
    'Name': '',
    'ConnectionType': '',
    'jdbcURL': '',
    'jdbcUsername': '',
    'jdbcPassword': '',
    'JDBC_ENFORCE_SSL': 'false',
}

crawlerDict = {
    'CrawlerName': '',
    'Role': '',
    'jdbcConnectionName': '',
    'jdbcConnectionPath': '',
    'S3Path': '',
    'GlueDatabaseName': '',
}


def glue_dict_file(fPath):
    filePath = r'%s' % fPath
    with open(filePath) as gluePromo:
        d = defaultdict(dict)
        current = 1
        file_contents = gluePromo.read()
        for line in file_contents.splitlines():
            if "=" in line:
                key, value = map(str.strip, line.split('='))
                if key in d[current]:
                    current += 1
                d[current][key] = value
    gluePromo.close()
    return d


def py_create_glue_job(glueDict):
    for key, val in pythonJobDict.items():
        x = 1
        if key in glueDict[x]:
            for k, v in glueDict[x].items():
                glueJobName = glueDict[x]['Name']
                glueJobDesc = glueDict[x]['Description']
                glueJobRole = glueDict[x]['Role']
                glueScript = glueDict[x]['ScriptLocation']
                glueTempDir = glueDict[x]['TempDir']
                gluePythonLib = glueDict[x]['PythonLibraryPath']
                glueEnvDir = glueDict[x]['envFilePath']
                glueCfgDir = glueDict[x]['cfgFilePath']
                glueWorkerT = glueDict[x]['WorkerType']
                glueWorkerCount = glueDict[x]['NumberOfWorkers']
                maxCapac = glueDict[x]['MaxCapacity']
                timeout = glueDict[x]['Timeout']
                #Pass glueWorkCount as int, Boto3 API only accepts INT variable for # of Glue Workers
                glueWorkersC = int(glueWorkerCount)
                python_boto3_job()
    print(glueJobName)
    x += 1


def python_boto3_job():
    print("Creating new Glue Job ......." + glueJobName)
    session = boto3.Session(profile_name=awsEnv)
    createGlueJob = client.create_job(
            Name=glueJobName,
            Description=glueJobDesc,
            Role=glueJobRole,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': glueScript,
                'PythonVersion': '2'
            },
            DefaultArguments={
                '--job-language': 'python',
                '--enable-metrics': " ",
                "--TempDir": glueTempDir,
                '--job-bookmark-option': 'job-bookmark-disable',
                '--extra-py-files': gluePythonLib,
                '--env_file_path': glueEnvDir,
                '--cfg_dir_path': glueCfgDir,

            },
            #Timeout=3600,
            #MaxCapacity=10,
            NumberOfWorkers=glueWorkersC,
            WorkerType=glueWorkerT,
            )
    print("Glue Job Created......." + glueJobName)
    return


def scala_create_glue_job(glueDict):
    for key, val in scalaJobDict.items():
        x = 1
        if key in glueDict[x]:
            for k, v in glueDict[x].items():
                glueJobName = glueDict[x]['Name']
                glueJobDesc = glueDict[x]['Description']
                glueJobRole = glueDict[x]['Role']
                glueScript = glueDict[x]['ScriptLocation']
                glueTempDir = glueDict[x]['TempDir']
                scalaClass = glueDict[x]['ScalaClass']
                jarFiles = glueDict[x]['ExtraJarFiles']
                glueWorkerT = glueDict[x]['WorkerType']
                glueWorkerCount = glueDict[x]['NumberOfWorkers']
                maxCapac = glueDict[x]['MaxCapacity']
                timeout = glueDict[x]['Timeout']
                #Pass glueWorkCount as int, Boto3 API only accepts INT variable for # of Glue Workers
                glueWorkersC = int(glueWorkerCount)
                scala_boto3_job()
    print(glueJobName)
    x += 1


def scala_boto3_job():
    print("Creating new Glue Job ......." + glueJobName)
    session = boto3.Session(profile_name=awsEnv)
    createGlueJob = client.create_job(
        Name=glueJobName,
        Description=glueJobDesc,
        Role=glueJobRole,
        Command={
            'Name': 'glueetl',
            'ScriptLocation': glueScript,
            'PythonVersion': '2'
        },
        DefaultArguments={'--TempDir': glueTempDir,
                          '--class': scalaClass,
                          '--extra-jars': jarFiles,
                          '--job-bookmark-option': 'job-bookmark-disable',
                          '--job-language': 'scala'},
        Timeout=timeout,
        MaxCapacity=maxCapac,
        NumberOfWorkers=glueWorkersC,
        WorkerType=glueWorkerT,
        GlueVersion='1.0',

    )
    print("Glue Job Created......." + glueJobName)
    return


def connection_create(glueDict):
    NonProdvpcProps = {'AvailabilityZone': 'ca-central-1b',
               'SecurityGroupIdList': ['sg-03518e82138ae8fed'],
               'SubnetId': 'subnet-0b35824546f34be2f'}

    for key, val in connectionDict.items():
        x = 1
        if key in glueDict[x]:
            for k, v in glueDict[x].items():
                glueConnName = glueDict[x]['Connection']['Name']
                glueconnDesc = glueDict[x]['Connection']['Description']
                glueConnType = glueDict[x]['Connection']['ConnectionType']
                glueConnProps = glueDict[x]['Connection']['ConnectionProperties']
                glueVPCpropsRead = glueDict[x]['Connection']['PhysicalConnectionRequirements']
                glueVPCpropsText = NonProdvpcProps
    return


def connection_boto3_create():
    # Create new Connection
    print('Creating New Connection.......' + glueConnName)
    session = boto3.Session(profile_name=awsEnv)
    glueCreateConn = client.create_connection(
        ConnectionInput={
            'Name': glueConnName,
            'Description': glueConnName,
            'ConnectionType': glueConnType,
            'ConnectionProperties': glueConnProps,
            'PhysicalConnectionRequirements': glueVPCprops,
        }
    )
    print('Connection Successfully Created.......' + glueConnName)
    return


def glue_crawler_create(glueDict):
    glueCrawlerName = str.replace(crwlParam['Crawler']['Name'], 'sunrise', 'amp')
    glueCrawlerRole = crwlParam['Crawler']['Role']
    glueCrawlerDB = str.replace(crwlParam['Crawler']['DatabaseName'], 'sunrise', 'amp')
    #glueCrawlerConn = glueCrawler['Crawler']['Connections']
    glueCrawlerTargets = crwlParam['Crawler']['Targets']
    #glueCrawlerTargetsAmp = [w.replace(glueCrawler['Crawler']['Targets'], 'sunrise', 'amp') for w in glueCrawlerTargets]
    glueSchemaChng = crwlParam['Crawler']['SchemaChangePolicy']

    print('Creating New Crawler.......' + glueCrawlerName)
    return


def crawler_boto3_create():
    glueCreate = client.create_crawler(
       Name=glueCrawlerName,
       Role=glueCrawlerRole,
       DatabaseName=glueCrawlerDB,
       Targets=glueCrawlerTargets,
    )
    print("Glue Crawler Created......." + glueCrawlerName)
    return


if __name__ == '__main__':
    env_input = input("What Environment are you working in?: \n" "1: Prod \n" "2: Stage \n" "3: NonProd \n" "4: Lab\n")
    awsEnv = input_glue_env(env_input)
    serv_input = input("What Glue Service are you working with?: \n" "1: Connection \n" "2: Crawler \n" "3: Job \n")
    glueServ = input_glue_service(serv_input)
    deploy_input = input("Are you creating new service or promotion?: \n" "1: Create \n" "2: Promotion \n")
    process = input_glue_process(deploy_input)
    fPath = input("Please provide full filepath to Template:").strip('\"')
