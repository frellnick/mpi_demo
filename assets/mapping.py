# Map ID cols to pool cols

colmap = {
    'STUDENT_ID': 'student_id_pool',
    'SSID': 'ssid_pool',
    'S_SSID': 'ssid_pool',
    'G_SSID': 'ssid_pool',
    'G_ID': 'student_id_pool',
    'S_ID': 'student_id_pool',
    'SC_ID': 'student_id_pool',
    'U_ID': 'student_id_pool',
    'SSN': 'ssn_pool',
    'S_SSN': 'ssn_pool',
    'SC_SSN': 'ssn_pool',
    'U_SSN': 'ssn_pool',
    'G_SSN': 'ssn_pool',
    'LAST_NAME': 'last_name_pool',
    'LASTNAME': 'last_name_pool',
    'S_LAST': 'last_name_pool',
    'U_LAST': 'last_name_pool',
    'U_LAST_NAME': 'last_name_pool',
    'G_LAST': 'last_name_pool',
    'FIRSTNAME': 'first_name_pool',
    'FIRST_NAME': 'first_name_pool',
    'S_FIRST': 'first_name_pool',
    'U_FIRST': 'first_name_pool',
    'U_FIRST_NAME': 'first_name_pool',
    'G_FIRST': 'first_name_pool',
    'MIDDLEINITIAL': 'middle_name_pool',
    'MIDDLE_NAME': 'middle_name_pool',
    'G_MIDDLE': 'middle_name_pool',
    'S_MIDDLE': 'middle_name_pool',
    'U_MIDDLE': 'middle_name_pool',
    'GENDER': 'gender_pool',
    'G_GENDER': 'gender_pool',
    'S_GENDER': 'gender_pool',
    'U_GENDER': 'gender_pool',
    'BIRTH_DATE': 'birth_date_pool',
    'G_BIRTH_DT': 'birth_date_pool',
    'U_BIRTH_DT': 'birth_date_pool',
    'S_BIRTH_DT': 'birth_date_pool',
    'guid':'guid',
}

local_identifiers = [
    'student_id_pool'
]

blocked_identifiers = [
    'student_id_pool',
    'ssn_pool',
    'ssid_pool',
]

sneighbourhood_identifiers = [
    'first_name_pool',
    'last_name_pool',
    'middle_name_pool',
    'gender_pool'
]