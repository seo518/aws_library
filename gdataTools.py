from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import google.auth.transport.requests as gRequests
from google.oauth2 import service_account as SA
from google.cloud import bigquery
from googleapiclient.discovery import build
from subprocess import call

import time, json, sys, os, pandas, math
import datetime

pd = pandas

os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "True"
SERVICE_ACCOUNT_FILE = 'Must be provided'
INSTALLED_CLIENT = 'Provide a client ID configured for installed application oauth'

class format:
    DCYAN = '\033[36m'
    PURPLE = '\033[95m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    RED = '\033[31m'
    DKGREY = '\033[90m'
    LTGREY = '\033[37m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


class fprinter:
    def __init__(self):
        pass

    def print_formatter(self, message, modifiers):
        idx = 0
        for idx in range(0, len(message)):
            if message[idx].isspace() == True:
                idx += 1
            else:
                break
        new_message = message[0:idx] + modifiers + message[idx:] + format.END
        print(new_message)

    def loud(self, message):
        self.print_formatter(message, format.DCYAN + format.BOLD + format.UNDERLINE)

    def blue(self, message):
        self.print_formatter(message, format.BLUE)

    def purple(self, message):
        self.print_formatter(message, format.PURPLE)

    def blue_bold(self, message):
        self.print_formatter(message, format.BOLD + format.BLUE)

    def warn(self, message):
        self.print_formatter(message, format.MAGENTA)

    def fail(self, message):
        self.print_formatter(message, format.BOLD + format.MAGENTA)

    def kill(self, message):
        self.print_formatter(message, format.BOLD + format.RED)

    def quiet(self, message):
        self.print_formatter(message, format.LTGREY)


FP = fprinter()
def FQ_BQ(project, dataset, table):
    return f"{project}.{dataset}.{table}"

def picker(list, selector="thing"):
    dict_set = {}
    idx = 1
    for item in list:
        if item[0] != '.':
            dict_set[idx] = item
            print("{0}. {1}".format(idx, item))
            idx += 1
    choice = int(input("Please pick the {0}.\t".format(selector)))
    print("\n")
    return dict_set[choice]


def chunk(data, chunksize):
    output = []
    i = math.ceil(len(data) / chunksize)
    for slice in range(0, i):
        output.append(data[50 * slice:(chunksize * slice) + chunksize])
    return output


def atomic_call(call, data_location='items', data_labels=None, backoff=1, **params):
    try:
        response = call(**params).execute()
        if data_labels:
            return response[data_labels], response[data_location]
        return response['items'], response['nextPageToken'] if 'nextPageToken' in response else None
    except Exception as e:
        print(e)
        if backoff < 5:
            try:
                if e.resp.status == 429:
                    time.sleep(3 ** (backoff + 1))
                    atomic_call(call, data_location=data_location, data_labels=data_labels, backoff=backoff + 1)
            except Exception as e:
                print("Couldnt get data {}".format(e))
                raise Exception(e)


def paged_call(call, **params):
    data = []
    nextPage = None
    while True:
        try:
            records, nextPage = atomic_call(call, **params)
            data += records
            if nextPage is None:
                break
            else:
                params['pageToken'] = nextPage
        except:
            break
    return data


def file_to_json(file):
    try:
        input = open(file, 'r')
    except Exception as e:
        FP.warn("File couldnt be opened: {}".format(e))
    try:
        return json.loads(input.read())
    except Exception as e:
        FP.warn("Couldnt convert to json: {}".format(e))
        return


class authenticatedClient:
    AUTH_STORAGE = './auth_storage/'
    SERVICE_ACCOUNT_FOLDER = './service_accounts/'

    def __init__(self, client_file=None, auth_storage=AUTH_STORAGE + 'dev.json', scopes=[]):
        self.cred_storage = auth_storage
        self.credentials_json = {}
        self.credentials = None
        self.client_file = (
                    client_file or self.SERVICE_ACCOUNT_FOLDER + ' installed.json')  # picker(os.listdir(self.SERVICE_ACCOUNT_FOLDER), "service_account")
        self.flow = InstalledAppFlow.from_client_secrets_file(
            self.client_file,
            scopes=scopes,
            redirect_uri='https://localhost')
        # Create a new client when needed
        self.client_spawn = gRequests.Request
        self.scopes = scopes

    def upsert_cred_file(self):
        self.credentials_json = {
            'access_token': self.credentials.token,
            'refresh_token': self.credentials.refresh_token,
            'id_token': self.credentials.id_token,
            'token_uri': self.credentials.token_uri,
            'client_id': self.credentials.client_id,
            'client_secret': self.credentials.client_secret,
            'expiry': str(self.credentials.expiry),
            'expiration': self.credentials.expiry.timestamp(),
            'scopes': self.credentials.scopes
        }
        # print("{} min left for token".format(int((int(self.credentials_json['expiration']) - int(time.time())) / 60)))

        with open(self.cred_storage, 'w') as c:
            c.write(json.dumps(self.credentials_json))

    def fresh_auth(self):
        # Create the flow using the client secrets file from the Google API
        # Console.
        self.credentials = self.flow.run_console()
        self.upsert_cred_file()

    def creds(self):
        try:
            self.credentials = Credentials.from_authorized_user_file(self.cred_storage)
            self.credentials.refresh(self.client_spawn())
            self.upsert_cred_file()

        except Exception as e:
            if self.credentials is not None:
                self.creds()
            else:
                self.fresh_auth()

        return self.credentials


DT = datetime.datetime.today().strftime('%Y%m%d')


def FILENAME(brand):
    return "{}_{}.{}.parquet".format(brand, DT, str(time.time()))


class saClient:
    def __init__(self, service_account_path, scopes=[]):
        self.sa_file = service_account_path
        self.scopes = scopes

    def creds(self, scopes=None):
        return SA.Credentials.from_service_account_file(self.sa_file, scopes=(scopes or self.scopes))

    def create_service(self, service, version=None, scopes=None, **kwargs):
        credentials = SA.Credentials.from_service_account_file(
            self.sa_file,
            scopes=(scopes or self.scopes))
        return build(service, version, credentials=credentials, **kwargs)


class clientServices:
    def __init__(self, creds=False, service_account=False, **kwargs):
        self.client = None
        if not (creds or service_account):
            raise Exception("Must specify at least 1: oAuth client credentials, or service account path")
        elif creds:
            self.client_file = creds
            self.auth = authenticatedClient(self.client_file, **kwargs)
            self.creds = self.auth.creds
            self.type = 'user'
        else:
            self.client_file = service_account
            self.auth = saClient(self.client_file, **kwargs)
            self.creds = self.auth.creds
            self.type = 'service'
        return

    def query_bq(self, query, gcp_project):
        bq = bigquery.client.Client(credentials=self.creds())
        bq.project = gcp_project
        results = bq.query(query=query, project=gcp_project).result().to_dataframe()
        return results

    def load_from_gs(self, source, dest, project='mp-content-intelligence'):
        try:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.PARQUET
            job_config.autodetect = True
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            bq = bigquery.Client.from_service_account_json(self.client_file)
            loader = bq.load_table_from_uri(
                source_uris=source,
                destination=dest,
                project=project,
                job_config=job_config)
            d = loader.result()
            d.done()
            FP.loud("loaded {} - {} rows".format(dest, d.output_rows))
            return d.output_rows
        except Exception as e:
            FP.warn("Couldnt update {}: {}".format(dest, e))
            return 0

    """
    These will become gtools
    """

    def analytics_call(self, report_args):

        ytClient = self.client
        report = report_args

        va = atomic_call(ytClient.reports().query, data_location='rows', data_labels='columnHeaders',
                         dimensions=report['dimensions'],
                         metrics=report['metrics'],
                         filters=report['filters'],
                         ids='channel=={}'.format(report['channelId']),
                         startDate=report['startDate'],
                         endDate=report['endDate']
                         )
        try:
            pdv = pandas.DataFrame(data=[r for r in va[1]], columns=[t['name'] for t in va[0]])

            if len(pdv) > 0:
                pdv['startDate'] = report['startDate']
                pdv['endDate'] = report['endDate']
                pdv['channel'] = report['channelId']
                pdv['period_type'] = report['periodType']
                pdv['period'] = report['period']
                for field in report['filters'].split(";"):
                    if field != '':
                        name, value = field.split("==")
                        pdv[name] = value
                for metric in report['metrics'].split(","):
                    pdv[metric] = pdv[metric].astype(float).fillna(0)
                pdv['execution_ts'] = int(time.time())
                pdv = pdv.rename(columns={"video": "videoId"})
                pdv.reset_index(drop=True).set_index(report['keys']).to_parquet(report['fileDest'], engine='pyarrow')
                FP.blue_bold(("Got metrics for {}".format(report['videoId'])))
                return pdv
            else:
                pass
        except Exception as e:
            FP.warn("Couldnt get data :  {}".format(e))
            return

    def get_authenticated_service(self, api_service_name, api_version):
        return build(api_service_name, api_version, credentials=self.creds())

    def atomic_call(call, data_location='items', data_labels=None, **params):
        response = call(**params).execute()
        if data_labels:
            return response[data_labels], response[data_location]
        return response['items'], response['nextPageToken'] if 'nextPageToken' in response else None

    def paged_call(call, **params):
        data = []
        nextPage = None
        while True:
            records, nextPage = atomic_call(call, **params)
            data += records
            if nextPage is None:
                break
            else:
                params['pageToken'] = nextPage
        return data

    def upsert_bq_from_df(self, dataset, table, data, gcp_project):
        bq = bigquery.client.Client(credentials=self.creds())
        bq.project = gcp_project
        MAX_RETRIES = 3
        full_retry = 0
        ds = None
        log = []
        while MAX_RETRIES > full_retry and not ds:
            try:
                upload_job = bq.load_table_from_dataframe(data, destination=FQ_BQ(gcp_project, dataset, table))
                upload_job.result()
                ds = upload_job.done()
                if ds:
                    print(upload_job.output_rows)
                    return upload_job.output_rows
                else:
                    time.sleep(2)
                    ds = upload_job.done()
            except Exception as e:
                log.append(e)
                try:
                    assert isinstance(e, AttributeError) == False
                    for ee in e.errors:
                        if ee['reason'] == 'rateLimitExceeded':
                            full_retry += 1
                            print("Hit {0} on {1}, waiting {2} seconds... ".format(ee['reason'], data.index[0][0],
                                                                                   6 * full_retry))
                            time.sleep(6 * (full_retry))
                        if ee['reason'] == 'quotaExceeded':
                            print("Hit {0} on {1} - F it.".format(ee['reason'], data.index[0][0]))
                            raise Exception(ee['reason'])
                except Exception as e:
                    # print(e)
                    full_retry = MAX_RETRIES
        # print(log)
        return log


if __name__ == "__main__":
    serviceSession = clientServices(service_account=SERVICE_ACCOUNT_FILE,
                                    scopes=[
                                        'https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/devstorage.full_control'])
    # print(serviceSession.creds())
    print(serviceSession.query_bq("SELECT 'yes' as is_bq_working", 'mp-content-intelligence'))

    installedClient = clientServices(creds=INSTALLED_CLIENT,
                                     auth_storage='auth_storage/ir.json',
                                     scopes=[
                                         'https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/devstorage.full_control'])
    # print(installedClient.creds())
    print(installedClient.query_bq("SELECT 'yes' as is_bq_working", 'mp-content-intelligence'))
