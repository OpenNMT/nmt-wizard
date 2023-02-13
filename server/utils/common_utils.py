import re
from datetime import datetime
from flask import g

from app import app


def get_pretty_error_message(error_message):
    format_error_messages = app.get_other_config(['format_error_messages'], fallback={})
    return format_error_messages.get(error_message, error_message)


def verify_resource_train_restricted(path, permission):
    if not is_resource_train_restricted(path):
        return True

    return check_permission_access_train_restricted(permission)


def check_permission_access_train_restricted(permission):
    # get entity code of user
    entity_code = None
    if g.get('user'):
        entity_code = g.user.entity.entity_code

    owner_code = app.get_other_config(['train_restricted', 'owner_code'])
    partner_codes = app.get_other_config(['train_restricted', 'partner_codes'])

    # user is OWNER, set full permission
    if owner_code is None or entity_code == owner_code:
        return True

    # user is PARTNER partners, set allowed permission
    if partner_codes is None or any(entity_code in partner['codes'] and permission in partner['permissions']
                                    for partner in partner_codes):
        return True

    return False


def is_resource_train_restricted(path):
    regex_pattern = r'\/train_restricted(\/.*)*$'

    return re.search(regex_pattern, path)


def get_expiration_status(expiration_date):
    time_delta = (expiration_date - datetime.now().date()).days
    if time_delta > 1:
        expiration_status = f"{time_delta} days left"
    elif time_delta == 1:
        expiration_status = "1 day left"
    else:
        expiration_status = "Expired"
    return expiration_status
