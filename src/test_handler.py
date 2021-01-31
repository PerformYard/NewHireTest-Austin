from handler import db, handle_csv_upload
import json
import datetime
import pymongo
import bcrypt
from bson import ObjectId


def dummy_data_decorator(test_function):
    def f():
        '''
        Drop any existing data and fill in some dummy test data,
        as well as creating indexes; the data will be dropped after
        the test as well
        '''

        db.user.drop()
        db.user.create_index([
            ("normalized_email", pymongo.ASCENDING),
        ], unique=True)

        dummy_users = [
            {
                "_id": ObjectId(),
                "name": "Brad Jones",
                "normalized_email": "bjones@performyard.com",
                "manager_id": None,
                "salary": 90000,
                "hire_date": datetime.datetime(2010, 2, 10),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(b"password",
                                                 bcrypt.gensalt()),
            },
            {
                "_id": ObjectId(),
                "name": "Ted Harrison",
                "normalized_email": "tharrison@performyard.com",
                "manager_id": None,
                "salary": 50000,
                "hire_date": datetime.datetime(2012, 10, 20),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(b"correct horse",
                                                 bcrypt.gensalt()),
            }
        ]

        # Give Ted a manager
        dummy_users[1]["manager_id"] = dummy_users[0]["_id"]

        for user in dummy_users:
            db.user.insert(user)

        db.chain_of_command.drop()
        db.chain_of_command.create_index([
            ("user_id", pymongo.ASCENDING),
        ], unique=True)

        dummy_chain_of_commands = [
            {"user_id": dummy_users[0]["_id"], "chain_of_command":[]},
            {"user_id": dummy_users[1]["_id"],
             "chain_of_command":[dummy_users[0]["_id"]]},
        ]

        for chain_of_command in dummy_chain_of_commands:
            db.chain_of_command.insert(chain_of_command)

        test_function()
        db.user.drop()
        db.chain_of_command.drop()
    return f


@dummy_data_decorator
def test_setup():
    '''
    This test should always pass if your environment is set up correctly
    '''
    assert(True)


@dummy_data_decorator
def test_simple_csv():
    '''
    This should successfully update one user and create one user,
    also updating their chain of commands appropriately
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Brad Jones,bjones@performyard.com,,100000,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 100000)

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_invalid_number():
    '''
    This test should still update Brad and create John, but should return
    a single error because the salary field for Brad isn't a number
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,NOT A NUMBER,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 1)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 90000)
    assert(brad["name"] == "Bradley Jones")

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_update_user():
    '''
        This test should update Brad's salary and Ted's hire date and manager
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Brad Jones,bjones@performyard.com,,,02/10/2010
Ted Harrison,tharrison@performyard.com,bjones@performyard.com,50000,07/16/2018
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 0)
    assert(body["numUpdated"] == 2)
    assert(len(body["errors"]) == 0)


@dummy_data_decorator
def test_create_users_bad_data():
    '''
        This test should create 4 users and test a piece
        of invalid data in each field giving 7 errors
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
,jdoe@performyard.com,,90000,02/10/2010
Jane Doe,,,90000,02/10/2010
J@ne Doe,jdoe@performyard.com,,90000,02/10/2010
Jane Doe,jdoe@performyardcom,,90000,02/10/2010
Jane Doe,jdoe@performyard.com,NOT EMAIL,90000,02/10/2010
Joe Book,jbook@performyard.com,,NUM,02/10/2010
Bob Dylan,bdylan@performyard.com,jbook@performyard.com,90000,02/101/2010
Mr Nothing,mnothing@performyard.com,bdylan@performyard.com,,
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 4)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 7)

    # Check the data for each user
    jane = db.user.find_one({"normalized_email": "jdoe@performyard.com"})
    jane_chain_of_command = db.chain_of_command.find_one(
        {"user_id": jane["_id"]})
    assert(jane["name"]) == "Jane Doe"
    assert(jane["normalized_email"]) == "jdoe@performyard.com"
    assert(jane["manager_id"]) is None
    assert(jane["salary"]) == 90000
    assert(jane["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(jane_chain_of_command["chain_of_command"]) == 0)

    joe = db.user.find_one({"normalized_email": "jbook@performyard.com"})
    joe_chain_of_command = db.chain_of_command.find_one(
        {"user_id": joe["_id"]})
    assert(joe["name"]) == "Joe Book"
    assert(joe["normalized_email"]) == "jbook@performyard.com"
    assert(joe["manager_id"]) is None
    assert(joe["salary"]) is None
    assert(joe["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(joe_chain_of_command["chain_of_command"]) == 0)

    bob = db.user.find_one({"normalized_email": "bdylan@performyard.com"})
    bob_chain_of_command = db.chain_of_command.find_one(
        {"user_id": bob["_id"]})
    assert(bob["name"]) == "Bob Dylan"
    assert(bob["normalized_email"]) == "bdylan@performyard.com"
    assert(bob["manager_id"]) == joe["_id"]
    assert(bob["salary"]) == 90000
    assert(bob["hire_date"]) is None
    assert(len(bob_chain_of_command["chain_of_command"]) == 1)

    mr = db.user.find_one({"normalized_email": "mnothing@performyard.com"})
    mr_chain_of_command = db.chain_of_command.find_one(
        {"user_id": mr["_id"]})

    assert(mr["name"]) == "Mr Nothing"
    assert(mr["normalized_email"]) == "mnothing@performyard.com"
    assert(mr["manager_id"]) == bob["_id"]
    assert(mr["salary"]) is None
    assert(mr["hire_date"]) is None
    assert(len(mr_chain_of_command["chain_of_command"]) == 2)


@dummy_data_decorator
def test_validate_name():
    '''
        This test should create 1 user and have 2 errors due to user name
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
,jdoe@performyard.com,,90000,02/10/2010
J D0e,jdoe@performyard.com,,90000,02/10/2010
Joe Book,jbook@performyard.com,,,02/10/2010
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 2)

    joe = db.user.find_one({"normalized_email": "jbook@performyard.com"})
    joe_chain_of_command = db.chain_of_command.find_one(
        {"user_id": joe["_id"]})
    assert(joe["name"]) == "Joe Book"
    assert(joe["normalized_email"]) == "jbook@performyard.com"
    assert(joe["manager_id"]) is None
    assert(joe["salary"]) is None
    assert(joe["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(joe_chain_of_command["chain_of_command"]) == 0)


@dummy_data_decorator
def test_valid_user_email():
    '''
        This test should create 1 user and have 2 errors due to user email
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
Jane Doe,jdoe@performyard,,90000,02/10/2010
Jane Doe,,,90000,02/10/2010
Joe Book,jbook@performyard.com,,9000,02/10/2010
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 2)

    joe = db.user.find_one({"normalized_email": "jbook@performyard.com"})
    joe_chain_of_command = db.chain_of_command.find_one(
        {"user_id": joe["_id"]})
    assert(joe["name"]) == "Joe Book"
    assert(joe["normalized_email"]) == "jbook@performyard.com"
    assert(joe["manager_id"]) is None
    assert(joe["salary"]) == 9000
    assert(joe["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(joe_chain_of_command["chain_of_command"]) == 0)


@dummy_data_decorator
def test_process_manager_id():
    '''
        This test should create 3 users with one error due to manger id
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
Jane Doe,jdoe@performyard.com,NOT EMAIL,90000,02/10/2010
Joe Book,jbook@performyard.com,,,02/10/2010
Bob Dylan,bdylan@performyard.com,jbook@performyard.com,90000,
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 3)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)

    # Check the data for each user
    jane = db.user.find_one({"normalized_email": "jdoe@performyard.com"})
    jane_chain_of_command = db.chain_of_command.find_one(
        {"user_id": jane["_id"]})

    joe = db.user.find_one({"normalized_email": "jbook@performyard.com"})
    joe_chain_of_command = db.chain_of_command.find_one(
        {"user_id": joe["_id"]})

    bob = db.user.find_one({"normalized_email": "bdylan@performyard.com"})
    bob_chain_of_command = db.chain_of_command.find_one(
        {"user_id": bob["_id"]})

    assert(jane["name"]) == "Jane Doe"
    assert(jane["normalized_email"]) == "jdoe@performyard.com"
    assert(jane["manager_id"]) is None
    assert(jane["salary"]) == 90000
    assert(jane["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(jane_chain_of_command["chain_of_command"]) == 0)

    assert(joe["name"]) == "Joe Book"
    assert(joe["normalized_email"]) == "jbook@performyard.com"
    assert(joe["manager_id"]) is None
    assert(joe["salary"]) is None
    assert(joe["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(joe_chain_of_command["chain_of_command"]) == 0)

    assert(bob["name"]) == "Bob Dylan"
    assert(bob["normalized_email"]) == "bdylan@performyard.com"
    assert(bob["manager_id"]) == joe["_id"]
    assert(bob["salary"]) == 90000
    assert(bob["hire_date"]) is None
    assert(len(bob_chain_of_command["chain_of_command"]) == 1)


@dummy_data_decorator
def test_process_salary():
    '''
        This test should create 3 users with one error due invalid
        salary data.
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
Jane Doe,jdoe@performyard.com,,90000,02/10/2010
Joe Book,jbook@performyard.com,,NUMBER,02/10/2010
Bob Dylan,bdylan@performyard.com,jbook@performyard.com,90000,
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 3)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)

    # Check the data for each user
    jane = db.user.find_one({"normalized_email": "jdoe@performyard.com"})
    jane_chain_of_command = db.chain_of_command.find_one(
        {"user_id": jane["_id"]})

    joe = db.user.find_one({"normalized_email": "jbook@performyard.com"})
    joe_chain_of_command = db.chain_of_command.find_one(
        {"user_id": joe["_id"]})

    bob = db.user.find_one({"normalized_email": "bdylan@performyard.com"})
    bob_chain_of_command = db.chain_of_command.find_one(
        {"user_id": bob["_id"]})

    assert(jane["name"]) == "Jane Doe"
    assert(jane["normalized_email"]) == "jdoe@performyard.com"
    assert(jane["manager_id"]) is None
    assert(jane["salary"]) == 90000
    assert(jane["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(jane_chain_of_command["chain_of_command"]) == 0)

    assert(joe["name"]) == "Joe Book"
    assert(joe["normalized_email"]) == "jbook@performyard.com"
    assert(joe["manager_id"]) is None
    assert(joe["salary"]) is None
    assert(joe["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(joe_chain_of_command["chain_of_command"]) == 0)

    assert(bob["name"]) == "Bob Dylan"
    assert(bob["normalized_email"]) == "bdylan@performyard.com"
    assert(bob["manager_id"]) == joe["_id"]
    assert(bob["salary"]) == 90000
    assert(bob["hire_date"]) is None
    assert(len(bob_chain_of_command["chain_of_command"]) == 1)


@dummy_data_decorator
def test_process_hire_date():
    '''
        This test should create 3 users with one error due a invalid
        hire data.
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
Jane Doe,jdoe@performyard.com,,90000,02/10/2010
Joe Book,jbook@performyard.com,,,
Bob Dylan,bdylan@performyard.com,jbook@performyard.com,90000,10/30/20201
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 3)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 1)

    # Check the data for each user
    jane = db.user.find_one({"normalized_email": "jdoe@performyard.com"})
    jane_chain_of_command = db.chain_of_command.find_one(
        {"user_id": jane["_id"]})

    joe = db.user.find_one({"normalized_email": "jbook@performyard.com"})
    joe_chain_of_command = db.chain_of_command.find_one(
        {"user_id": joe["_id"]})

    bob = db.user.find_one({"normalized_email": "bdylan@performyard.com"})
    bob_chain_of_command = db.chain_of_command.find_one(
        {"user_id": bob["_id"]})

    assert(jane["name"]) == "Jane Doe"
    assert(jane["normalized_email"]) == "jdoe@performyard.com"
    assert(jane["manager_id"]) is None
    assert(jane["salary"]) == 90000
    assert(jane["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(jane_chain_of_command["chain_of_command"]) == 0)

    assert(joe["name"]) == "Joe Book"
    assert(joe["normalized_email"]) == "jbook@performyard.com"
    assert(joe["manager_id"]) is None
    assert(joe["salary"]) is None
    assert(joe["hire_date"]) is None
    assert(len(joe_chain_of_command["chain_of_command"]) == 0)

    assert(bob["name"]) == "Bob Dylan"
    assert(bob["normalized_email"]) == "bdylan@performyard.com"
    assert(bob["manager_id"]) == joe["_id"]
    assert(bob["salary"]) == 90000
    assert(bob["hire_date"]) is None
    assert(len(bob_chain_of_command["chain_of_command"]) == 1)


@dummy_data_decorator
def test_create_users():
    '''
        This test should still create both Brad and John with no errors
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
Bob Dylan,bdylan@performyard.com,,90000,02/10/2010
John Smith,jsmith@performyard.com,bdylan@performyard.com,80000,07/16/2018
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 2)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 0)

    # Not clearing just to keep test data to view
    # db.user.drop()
    # db.chain_of_command.drop()


@dummy_data_decorator
def test_create_chain_of_command():
    '''
        This test should create a chain of command of bill-> john -> bob
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
Bob Dylan,bdylan@performyard.com,,90000,02/10/2010
John Smith,jsmith@performyard.com,bdylan@performyard.com,80000,07/16/2018
Bill Joe,BjOe@performyard.com,jsmith@performyard.com,,05-30-2020
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 3)
    assert(body["numUpdated"] == 0)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 5)
    assert(db.chain_of_command.count() == 5)

    # Checking bill's chain of command
    bob = db.user.find_one({"normalized_email":
                            "bdylan@performyard.com"})["_id"]
    john = db.user.find_one({"normalized_email":
                             "jsmith@performyard.com"})["_id"]
    chain_ids = [bob, john]

    bill = db.user.find_one({"normalized_email": "bjoe@performyard.com"})
    bill_chain_of_command = db.chain_of_command.find_one(
        {"user_id": bill["_id"]})

    assert(len(bill_chain_of_command["chain_of_command"]) == 2)
    assert(all(ids in chain_ids for
               ids in bill_chain_of_command["chain_of_command"]))


@dummy_data_decorator
def test_search_for_manager():
    '''
        This test should find Joe's manager "Mr nothing"
        and create a skeleton user for him and then update
        the skeleton.
    '''
    body = '''Name,Email,Manager,Salary,Hire Date
Jane Doe,jdoe@performyard.com,,90000,02/10/2010
Joe Book,jbook@performyard.com,mnothing@performyard.com,,02/10/2010
Bob Dylan,bdylan@performyard.com,,90000,
Mr Nothing,mnothing@performyard.com,bdylan@performyard.com,,
    '''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    # Due to design of searching for a manager Joe is an update
    assert(body["numCreated"] == 3)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check the data for each user
    jane = db.user.find_one({"normalized_email": "jdoe@performyard.com"})
    jane_chain_of_command = db.chain_of_command.find_one(
        {"user_id": jane["_id"]})

    joe = db.user.find_one({"normalized_email": "jbook@performyard.com"})
    joe_chain_of_command = db.chain_of_command.find_one(
        {"user_id": joe["_id"]})

    bob = db.user.find_one({"normalized_email": "bdylan@performyard.com"})
    bob_chain_of_command = db.chain_of_command.find_one(
        {"user_id": bob["_id"]})

    mr = db.user.find_one({"normalized_email": "mnothing@performyard.com"})
    mr_chain_of_command = db.chain_of_command.find_one(
        {"user_id": mr["_id"]})

    assert(jane["name"]) == "Jane Doe"
    assert(jane["normalized_email"]) == "jdoe@performyard.com"
    assert(jane["manager_id"]) is None
    assert(jane["salary"]) == 90000
    assert(jane["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(jane_chain_of_command["chain_of_command"]) == 0)

    assert(joe["name"]) == "Joe Book"
    assert(joe["normalized_email"]) == "jbook@performyard.com"
    assert(joe["manager_id"]) == mr["_id"]
    assert(joe["salary"]) is None
    assert(joe["hire_date"]) == datetime.datetime(2010, 2, 10)
    assert(len(joe_chain_of_command["chain_of_command"]) == 2)

    assert(bob["name"]) == "Bob Dylan"
    assert(bob["normalized_email"]) == "bdylan@performyard.com"
    assert(bob["manager_id"]) is None
    assert(bob["salary"]) == 90000
    assert(bob["hire_date"]) is None
    assert(len(bob_chain_of_command["chain_of_command"]) == 0)

    assert(mr["name"]) == "Mr Nothing"
    assert(mr["normalized_email"]) == "mnothing@performyard.com"
    assert(mr["manager_id"]) == bob["_id"]
    assert(mr["salary"]) is None
    assert(mr["hire_date"]) is None
    assert(len(mr_chain_of_command["chain_of_command"]) == 1)
