import codecs
import json
import os
import smtplib
from email.mime.text import MIMEText
from string import Template
from nmtwizard import configuration as config

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
    def get_email_body_template(cls, template_type):
        types_dict = {
            'advanced': 'email_template_mode_advanced.html',
            'lite': 'email_template_mode_lite.html'
        }
        file_name = types_dict.get(template_type)
        f = codecs.open(os.path.dirname(os.path.realpath(__file__)) + '/email_template/' + file_name, 'r')
        return f.read()


def send_task_status_notification_email(task_infos, status):
    task_id = task_infos["id"]
    task_type = task_infos.get("type") if task_infos.get("type") else 'push model'
    mode = system_config['application']['mode'] if system_config.get('application') else 'advanced'
    content = json.loads(task_infos["content"])
    trainer_email = content.get("trainer_email")
    trainer_name = content.get("trainer_name")
    receiver = system_config['email']['receiver']
    receiver.append(trainer_email)
    eval_model = task_infos.get("eval_model")
    model = task_infos.get("model") if task_infos.get("model") else task_infos.get("parent")
    model_name = content.get("name")
    eval_name = ''
    link_info = ''
    if mode == 'advanced':
        subject = 'task'
        if task_type == 'push model':
            link_info = 'Model: ' + model
            link = system_config['email']['url'] + 'model/detail/' + model
        else:
            link_info = 'Task_id: ' + task_id
            link = system_config['email']['url'] + 'task/detail/' + task_id
    else:
        link = system_config['email']['url'] + 'model/detail/' + str(model)
        if eval_model:
            model_name = content.get("eval_model_input_name")
            link = system_config['email']['url'] + 'evaluation'
            subject = 'model evaluation'
            eval_name = 'Evaluation name: ' + content.get("eval_name") + '<br>'
        elif task_type in ('prepr', 'train'):
            subject = 'model training'
        elif task_type in ('relea', 'push model'):
            model_name = content.get("model_input_name")
            subject = 'model deploying'
        else:
            subject = 'model scoring'

    subject_status = 'completed' if status == 'completed' else 'failed'
    email_subject = (subject + ' ' + subject_status).upper()
    body_template = EmailUtils.get_email_body_template(template_type=mode)
    email_body = Template(body_template).safe_substitute(link_info=link_info, task_type=task_type, status=status,
                                                         last_name=trainer_name, link=link, subject=subject,
                                                         model_name=model_name, eval_name=eval_name)

    EmailUtils.send_text_mail(email_subject, email_body, receiver)
