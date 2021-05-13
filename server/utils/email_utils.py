from flask import current_app as app
import smtplib
from email.mime.text import MIMEText


MAIL_SERVER = {
    "gmail": ("smtp.gmail.com", 587),
    "office365": ("smtp.office365.com", 587)
}


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
        message = MIMEText(body)
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
        email_sender_config = app.get_other_config(["email_sender"])
        return email_sender_config

    @classmethod
    def get_sender(cls):
        sender_info = cls.get_sender_info()
        sender = EmailSender(sender_info["username"], sender_info["password"], sender_info["type"], sender_info.get("display_name"))
        return sender
