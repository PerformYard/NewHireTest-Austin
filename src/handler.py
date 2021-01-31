import json
import os
import bcrypt
import datetime
import re
import csv

from pymongo import MongoClient

from dateutil.parser import parse

db_uri = os.environ.get("MONGO_DB_URI", "localhost")
db_name = os.environ.get("MONGO_DB_NAME", "new_hire_test")

db = MongoClient(db_uri)[db_name]


def process_hire_date(date, response_body):
    '''
        Tries to create a hire date using parse from dateutil.parse
    '''
    if date == "":
        return date

    hire_date = None
    # Just catching a general exception since it is due to bad input
    try:
        hire_date = parse(date.strip())
    except(Exception):
        # This would normally be logged
        response_body["errors"].append(f"{date}: is not a valid date")
        return None

    return hire_date


def process_salary(value, response_body):
    '''
        Tries to create a salary based on the passed value
    '''
    salary = None
    try:
        if isinstance(value, str):
            salary = value.strip()
            if salary == "":
                return ""
            salary = int(salary)
        # Since everything is from a CSV the two cases below should not be
        # possible but are other accounting for if the input ever changed
        elif isinstance(value, int):
            return salary
        else:
            salary = int(value)
    except(Exception):
        response_body["errors"].append(f"{value}: is not a valid salary")
        return None

    return salary


def process_manager_id(manager_email, response_body):
    '''
        Checks to see if manager_email is a valid email and if a user
        exists with an email the same as manager_email.
        Returns either the Id of the user object with manager_email
        or None because the data is invalid or the user doesn't exist
    '''
    manager_id = None
    manager_email = validate_email(manager_email)

    if manager_email is None:
        response_body["errors"].append(f"{manager_email}: is not a valid "
                                       + "manager id.")
        return None
    elif manager_email == "":
        return manager_email

    manager = db.user.find_one({"normalized_email": manager_email})
    if manager is not None:
        manager_id = manager["_id"]

    return manager_id


def validate_user_email(user_email, response_body):
    '''
        This function is here for user email error checking without
        adding the logic to validate_email which is used by other functions
        and wouldn't be correct for them.
    '''
    email = validate_email(user_email)

    if email is None:
        response_body["errors"].append(f"{user_email}: is not "
                                       + "a user valid email")
    elif email == "":
        response_body["errors"].append("user email was blank")

    return email


def validate_email(email):
    '''
        Verifies if the email is valid and
        returns a cleaned copy or an empty string
    '''
    if email is None:
        return email

    email = email.lower().strip()
    if email == "":
        return email

    regex = r'^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
    if(re.search(regex, email)):
        return email

    return None


def validate_name(value, response_body):
    '''
        Verifies if the passed name is valid and returns a cleaned copy
    '''
    # Using a fairly simple regex to make sure it only has letters and spaces
    # If something more advanced was need I'd go with the following lib or
    # something like it:
    # https://pypi.org/project/nameparser/
    name = None
    if value == "":
        response_body["errors"].append("user name was blank")
        return None

    regex = r'([A-Za-z]+)( [A-Za-z]+)*( [A-Za-z]+)*$'
    name = value.strip()
    if(re.fullmatch(regex, name)):
        return name

    response_body["errors"].append(f"{name}: is not a valid name")
    return None


def create_user(name, email, manager_id, salary, hire_date):
    '''
        Creates a new user based on the passed values
    '''
    # This check allows us to determine if data was invalid or blank
    if salary == "":
        salary = None
    if manager_id == "":
        manager_id = None
    if hire_date == "":
        hire_date = None

    user = {
            "name": name,
            "normalized_email": email,
            "manager_id": manager_id,
            "salary": salary,
            "hire_date": hire_date,
            "is_active": True,
            "hashed_password": bcrypt.hashpw(b"password", bcrypt.gensalt()),
    }
    db.user.insert_one(user)


def update_user(name, email, manager_id, salary, hire_date):
    '''
        Updates user based on the passed values
    '''
    # Name should be valid at this point no matter what
    query_filter = {"normalized_email": email}
    new_values = {"$set": {"name": name}}

    # Appending values to update if they are valid to update
    if salary is not None:
        # This extra check allows us to determine if data was invalid or blank
        if salary == "":
            salary = None

        d = new_values["$set"]
        d["salary"] = salary

    if manager_id is not None:
        if manager_id == "":
            manager_id = None

        d = new_values["$set"]
        d["manager_id"] = manager_id

    if hire_date is not None:
        if hire_date == "":
            hire_date = None

        d = new_values["$set"]
        d["hire_date"] = hire_date

    db.user.update_one(query_filter, new_values)


def create_chain_of_command(user_email, manager_id):
    '''
        Creates a chain_of_command based on user_email and manager_email
    '''
    managers = []
    user_id = db.user.find_one({"normalized_email": user_email})["_id"]

    if manager_id is not None and manager_id != "":
        managers = get_chain_of_command(manager_id)

    chain_of_command = {
        "user_id": user_id,
        "chain_of_command": managers
    }

    db.chain_of_command.insert(chain_of_command)


def get_chain_of_command(manager_id):
    '''
        Returns the user's chain of command
    '''
    # Need to check if this manager reports to anyone
    managers_to_search = [manager_id]

    # We know they have at least one manager
    result = [manager_id]

    while len(managers_to_search) != 0:
        _id = managers_to_search[0]
        del managers_to_search[0]

        manager = db.chain_of_command.find_one({"user_id": _id})
        if manager is None:
            return result

        for x in manager["chain_of_command"]:
            managers_to_search.append(x)
            result.append(x)

    return result


def search_csv_for_manager(reader, manager_id):
    '''
        Searches the CSV for a undefined manager and creates a skeleton of the
        user and returns the new user's object Id. This new record will be
        updated later on when the main loop finds the record and updates it
    '''
    # Note: I am not sure how I feel about this approach because records that
    # need this become updates instead of creates.
    # This could be changed to create the full user but would require some
    # refactoring of how we process each line's data to allow this to
    # become recursive. As a situation could arise where we need to create
    # a manager for this and follow path until we reach the last manager.
    manager_id = manager_id.lower().strip()

    for line in reader:
        manager = line["Manager"].lower().strip()
        # Creating a basic user that will be updated later
        if manager == manager_id:
            user = {
                "name": "",
                "normalized_email": manager_id,
                "manager_id": None,
                "salary": None,
                "hire_date": None,
                "is_active": True,
                "hashed_password": bcrypt.hashpw(b"password",
                                                 bcrypt.gensalt()),
            }
            _id = db.user.insert_one(user)
            return _id.inserted_id

    # Couldn't find user in CSV
    return None


def handle_csv_upload(event, context):
    '''
        Transforms CSV data into user's and chain_of_command objects
    '''
    response_body = {
        "numCreated": 0,
        "numUpdated": 0,
        "errors": [],
    }

    # Splitting on newlines to simulate lines
    lines = event.split("\n")
    # The empty last line that is added from the split
    del lines[len(lines) - 1]

    # ['Name', 'Email', 'Manager', 'Salary', 'Hire Date']
    # John Smith, jsmith@pyard.com, bjones@pyard.com, 80000, 07/16/2018
    reader = csv.DictReader(lines, delimiter=',')

    for line in reader:

        # The fields below cannot be null
        name = validate_name(line["Name"], response_body)
        email = validate_user_email(line["Email"], response_body)

        # The fields below can be null
        manager_id = process_manager_id(line["Manager"], response_body)

        # If manager Id is None but the email is valid
        # then the id is valid but we didn't find a user
        # and we need to search for one
        if manager_id is None:
            _id = validate_email(line["Manager"])
            if _id is not None:
                new_reader = csv.DictReader(lines, delimiter=',')
                manager_id = search_csv_for_manager(new_reader, _id)

                if manager_id is None:
                    response_body["errors"].append(f"{_id}: "
                                                   + "is not a valid manager "
                                                   + "id.")

        salary = process_salary(line["Salary"], response_body)
        hire_date = process_hire_date(line["Hire Date"], response_body)

        # This check is here to allows us to log errors in user data but not
        # create or update anything due to the name or email not being valid
        if email is None or email == "" or name is None or name == "":
            continue

        user = None
        user = db.user.find_one({"normalized_email": email})

        # Did we find an existing user to update or do we need to create one
        # Also making sure we don't get anything strange like a valid email
        # to update with but an invalid user name to change.
        if user is None:
            create_user(name, email, manager_id, salary, hire_date)
            response_body["numCreated"] += 1
        elif user is not None:
            update_user(name, email, manager_id, salary, hire_date)
            response_body["numUpdated"] += 1

    # Time to create the chain_of_command after we have ran through the CSV
    # this removes the complexity of having to build and update them as we
    # create users
    # This could be optimized by tracking new users and which
    # existing ones had manager changes
    users = db.user.find()

    for user in users:
        email = user["normalized_email"]
        manager_id = user["manager_id"]
        db.chain_of_command.delete_one({"user_id": user["_id"]})
        create_chain_of_command(email, manager_id)

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response
