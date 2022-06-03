import requests
import base64
import json
from config import CODE_ERROS_ENVIAR_BACKUP
from os import getenv


USER_CV = getenv('USER_CV')
PASSWORD_CV = getenv('PASSWORD_CV')
FILA_BACKUP = 'fila_backup' #email da fila de backup
FILARESPONSAVEL_BY_AGENTTYPE = {
    "Windows File System": "Operação de Sistemas Operacionais e Componentes de Infra",
    "Linux File System": "Operação de Sistemas Operacionais e Componentes de Infra",
    "SQL Server": "Banco de Dados",
    "Oracle Database": "Banco de Dados",
    "Oracle RAC": "Banco de Dados",
    "Virtual Server": "Operação de Sistemas Operacionais e Componentes de Infra" #VM - MAQUINAS VIRTUAIS
}

ID_GRUPOTELEGRAM_BY_FILA = {
    "Operação de Sistemas Operacionais e Componentes de Infra": "-692507255",
    "Banco de Dados": "-1001514508170",
}

def token_cv() -> str:
    password_cv_bytes = PASSWORD_CV.encode('ascii')
    password_cv_base64_bytes = base64.b64encode(password_cv_bytes)
    password_cv_base64_str = password_cv_base64_bytes.decode('ascii')

    url = "http://10.134.160.2/webconsole/api/Login"

    payload = json.dumps({
        "password": password_cv_base64_str,
        "username": USER_CV
    })
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload) #requisição http
    response_dict = json.loads(response.text)

    return response_dict['token']


def get_commvault_jobs() -> list:

    url = "http://10.134.160.2/webconsole/api/Jobs"

    payload = "{}"
    headers = {
        'Accept': 'application/json',
        'Authtoken': token_cv(),
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response_dict = response.json()
    all_jobs = response_dict.get("jobs", list())
    jobs_pending = list()
    for job in all_jobs:
        job_summary = job.get("jobSummary")
        if job_summary.get("status").upper() == 'FAILED':
            job_info = dict()
            job_info["appTypeName"] = job_summary.get("appTypeName")
            job_info["jobId"] = job_summary.get("jobId")
            job_info["destClientName"] = job_summary.get("destClientName")
            job_info["status"] = job_summary.get("status")
            job_info["pendingReason"] = job_summary.get("pendingReason")
            job_info["errorcode"] = job_summary.get("pendingReasonErrorCode")
            job_info["backupLevelName"] = job_summary.get("backupLevelName")
            jobs_pending.append(job_info)

    return jobs_pending


def configurar_fila() -> list:
    jobs_com_fila = list()
    for job in get_commvault_jobs():
        app_name = job.get("appTypeName")
        if job.get("errorcode") in CODE_ERROS_ENVIAR_BACKUP:
            fila_name = FILA_BACKUP
        else:
            fila_name = FILARESPONSAVEL_BY_AGENTTYPE.get(app_name)
        if fila_name:
            job["fila"] = fila_name
            jobs_com_fila.append(job)

    return jobs_com_fila


def send_message_telegram(messages):
    url = "http://api.notificationhub.homolog.globoi.com/telegram/send-message"

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    for message in messages:
        payload = {
            "chat_id": message.get("chat_id"),
            "message": message.get("message")
        }
        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        print(response)


# def make_messages_telegram(jobs):
#     messages = list()
#     messages_by_group_telegram = dict()
#     for job in jobs:
#         nome_da_fila = job.get("fila")
#         group_telegram = ID_GRUPOTELEGRAM_BY_FILA.get(nome_da_fila)
#         telegram_message = f'Job ID:   {job.get("jobId")}\nServidor:   {job.get("destClientName")}\nAgent:   {job.get("appTypeName")}\nStatus:   {job.get("status")}\nJob Type:   {job.get("backupLevelName")}\n\nMotivo:   {job.get("pendingReason")}\nFila:   {job.get("fila")}\nErrorCode:   {job.get("errorcode")}\n \n\n\n'
#         if not messages_by_group_telegram.get(group_telegram):
#             messages_by_group_telegram[group_telegram] = telegram_message
#         else:
#             messages_by_group_telegram[group_telegram] = messages_by_group_telegram[group_telegram] + telegram_message
#     for key, value in messages_by_group_telegram.items():
#         messages.append({"chat_id": key, "message": value})
#
#     return messages

def make_messages_telegram(jobs):
    messages = list()
    for job in jobs:
        nome_da_fila = job.get("fila")
        group_telegram = ID_GRUPOTELEGRAM_BY_FILA.get(nome_da_fila)
        telegram_message = f'Job ID:   {job.get("jobId")}\nServidor:   {job.get("destClientName")}\nAgent:   {job.get("appTypeName")}\nStatus:   {job.get("status")}\nJob Type:   {job.get("backupLevelName")}\n\nMotivo:   {job.get("pendingReason")}\nFila:   {job.get("fila")}\nErrorCode:   {job.get("errorcode")}\n \n\n\n'
        messages.append({"chat_id": group_telegram, "message": telegram_message})

    return messages


if __name__ == '__main__': #VERIFICA SE O ARQUIVO ESTA SENDO EXECUTADO
    jobs_pending = make_messages_telegram(configurar_fila())

    send_message_telegram(jobs_pending)


