import builtins
import codecs
import json
import os
import smtplib
from email.mime.text import MIMEText
from string import Template
from nmtwizard import configuration as config
from app.routes import get_input_name

MAIL_SERVER = {
    "gmail": ("smtp.gmail.com", 587),
    "office365": ("smtp.office365.com", 587)
}
system_config = config.get_system_config()


class EmailSender:
    def __init__(self, username, password, email_type, display_name):
        self.username = username
        self.password = password
        self.email_type = email_type
        self.display_name = display_name
        self.server = None

    def login(self):
        server_address = MAIL_SERVER.get(self.email_type)
        if not server_address:
            raise Exception(f"Invalid type: {self.email_type}")
        server = smtplib.SMTP(*server_address)
        server.starttls()
        server.login(self.username, self.password)
        self.server = server

    def quit(self):
        self.server.quit()

    def send_mail(self, receivers, email):
        email_string = email.as_string()
        self.server.sendmail(self.username, receivers, email_string)


class Email:
    def __init__(self, subject, body, sender_display_name, receivers):
        message = MIMEText(body, "html")
        message['Subject'] = subject
        message['From'] = sender_display_name
        message['To'] = ", ".join(receivers)

        self.message = message

    def as_string(self):
        return self.message.as_string()


class EmailUtils:
    @staticmethod
    def send_text_mail(subject, body, receivers, sender=None, logout_after_send=True):
        if not sender:
            logout_after_send = True
            sender = EmailUtils.get_sender()

        sender.login()
        email = Email(subject, body, sender.display_name, receivers)
        sender.send_mail(receivers, email)
        if logout_after_send:
            sender.quit()

    @classmethod
    def get_sender_info(cls):
        email_sender_config = system_config["email"]
        return email_sender_config

    @classmethod
    def get_sender(cls):
        sender_info = cls.get_sender_info()
        sender = EmailSender(sender_info["username"], sender_info["password"], sender_info["type"],
                             sender_info.get("display_name"))
        return sender

    @classmethod
    def get_email_body_template(cls, mode):
        file_name = 'email_template_mode_advanced.html' if mode == 'advanced' else 'email_template_mode_lite.html'
        f = codecs.open(os.path.dirname(os.path.realpath(__file__)) + '/email_template/' + file_name, 'r')
        return f.read()


def send_task_status_notification_email(task_infos, status):
    task_id = task_infos["id"]
    task_type = task_infos.get("type")
    mode = system_config['application']['mode']
    if mode == 'lite' and status == 'completed' and task_type in ('preprocess', 'trans'):
        return

    content = json.loads(task_infos["content"])
    trainer_email = content.get("trainer_email")
    trainer_name = content.get("trainer_name")
    receiver = system_config['email']['receiver']
    receiver.append(trainer_email)
    eval_model = task_infos.get("eval_model")
    model = task_infos.get("model")
    model_name = content.get("name")
    eval_name = ''
    if mode == 'advanced':
        link = system_config['email']['url'] + 'task/detail/' + task_id
        subject = 'task'
    else:
        if eval_model:
            ok, model_info = builtins.pn9model_db.catalog_get_info(eval_model, True)
            model_name = get_input_name(model_info)
            link = system_config['email']['url'] + 'evaluation'
            subject = 'model evaluation'
            eval_name = 'Evaluation name: ' + content.get("eval_name") + '<br>'
        else:
            link = system_config['email']['url'] + 'model/detail/' + str(model)
            subject = 'model training' if task_type in ('preprocess', 'train') else 'model scoring'

    subject_status = 'completed' if status == 'completed' else 'failed'
    email_subject = (subject + ' ' + subject_status).upper()
    body_template = EmailUtils.get_email_body_template(mode)
    email_body = Template(body_template).safe_substitute(task_id=task_id, task_type=task_type, status=status,
                                                         last_name=trainer_name, link=link, subject=subject,
                                                         model_name=model_name, eval_name=eval_name)

    EmailUtils.send_text_mail(email_subject, email_body, receiver)
