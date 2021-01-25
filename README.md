# PerformYard New Hire Test

This project contains an incomplete AWS Lambda function meant to be completed by potential new hires at PerformYard to evaluate their python programming skills.

## The Test:

Your job is a to write the function body of an AWS Lambda job (don't worry, you don't actually have to deploy this anywhere, we'll do all testing locally) which does the following:

- Accepts a CSV upload of user data from a customer (this data is expected to be user generated, and so could be malformed)
- For each row in the CSV:
 - Try to find an existing user in the database based on the upload's "Email" column and update that user's data in the database to match the upload (assume the data is valid)
 - Otherwise create a new user from the uploaded data
- In addition to user data, the database also contains a "chain_of_command" collection which store an object for each user which contains a list of all user's ids who are above the given user in the company's org chart.  That is, for a user X, X's chain of command contains the ids of X's manager, and X's manager's manager, etc...  This collection should be updated to reflect any changes from the uploaded user data as well.

### Specifics

#### Schemas

The database will contain two collections: "user" and "chain_of_command".  The user collection contains objects of the form:

```
{
    '_id': ObjectId('5b89a30294c76a231886617f'),
    'name': 'Brad Jones',
    'normalized_email': 'bjones@performyard.com',
    'manager_id': ObjectId('5b89a30294c76a2318866100'),
    'salary': 90000,
    'hire_date': datetime.datetime(2010, 2, 10, 0, 0),
    'is_active': True,
    'hashed_password': b'$2b$12$0.ozxSVoLdcMt2tjkTUl/./6L6OIcKE7yoO2jdkW4FQff4pL/7Oii'
}
```

_id and manager_id fields should be mongo's ObjectId type, name and email should be strings (and emails should be valid emails), salary should be a number, hire_date should be a valid date object, is_active should be a boolean, and hashed_password is binary data representing a password hashed in bcrypt.  The manager_id, salary, and hire_date are not require, however, and may be set to null.  The _id and normalized_email fields have unique indices, so there are guarranteed to be no duplicates.

The chain_of_command collection contains objects of the form:

```
{
    '_id': ObjectId('5b89a30294c76a2318866134'),
    'user_id': ObjectId('5b89a30294c76a231886617f'),
    'chain_of_command': [ObjectId('5b89a30294c76a2318866102'), ObjectId('5b89a30294c76a2318866100')]
}
```

The _id and user_id are ObjectIds, and the chain_of_command field is a list of users' ids, representing the ids of users above the user (given by the user_id field) in the company's managerial structure. The _id and normuser_id fields have unique indices, so there are guaranteed to be no duplicates.

#### Constraints

- After running the handle_csv_upload function, all fields in the database should maintain their correct type (salary should either be a number or null, never a string, for example) and only the name, salary, manager_id, and hire_date fields should be changed based on the user data.
- The normalized_email field should be filled in for new users, but since email is used to match, it should never change for existing users as a result of this function.
- Your function should make a best effort to parse the given data and should update any fields it is able to, even if other fields are not parsable due to user error.

## Setup

### Prerequisites

By default, the handler function expects an instance of mongodb (preferably 3.6 or newer) to be running on localhost on the default port of 27017 (this can be configured by setting an envirnomental variable of `MONGO_DB_URI` to be the hostname string of the mongodb instance.)  We use python3 (but python 2 probably works as well).  We also recommend using virtualenv with pip or something similar to install the required python libraries found in `requirements.txt`.

### Installing

Install mongodb (here for Ubuntu, but you can use brew on OSX)

```
apt install mongodb
```

Install virtualenv

```
apt install python-virtualenv
cd ./src
virtualenv -p python3 venv
```

To activate the virtualenv, run

```
source venv/bin/activate
```

Install the required python libraries

```
pip install -r requirements.txt
```

Run the test

```
pytest
```

If you've install everything correctly, the first test should pass, all other tests should fail until you write the correct code in the `handle_csv_upload` function in `handler.py`.


## Running the tests

We use pytest to run our tests, so running `pytest` in the src directory should run all tests.  We have included some simple tests to get you started, __but you are encouraged to write your own tests to handle edge cases which our tests do not cover.__  The tests as written will clear the database between each run, so use a different database or modify the tests if you wish to persist data.


### And coding style tests

```
PEP8 should be used to format all python code
```

