from pymongo import MongoClient
from functools import wraps
from time import process_time


def measure(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        trials = []
        for i in range(1000):
            start = process_time() * 1000
            res = func(*args, **kwargs)
            end_ = process_time() * 1000 - start
            trials.append(end_)

        mean = sum(trials) / len(trials)
        print(
            f"Total execution time {func.__name__}: {mean if mean > 0 else 0} ms"
        )

    return _time_it


client = MongoClient("mongodb://root:example@localhost:27017/")
db = client['titanic_db']  # Create or use an existing database
collection = db['passengers']  # Create or use an existing collection


@measure
def get_all():
    return list(collection.find())


@measure
def get_old_females():
    return list(collection.find({"Sex": "female", "Age": {"$gt": 30}}))


@measure
def get_all_names_starting_with_m():
    return list(collection.find({"Name": {"$regex": "^M"}}))


@measure
def make_all_males_age_30():
    return collection.update_many({"Sex": "male", "Age": {"$gt": 30}}, {"$set": {"Age": 30}})


if __name__ == '__main__':
    get_all()
    get_old_females()
    get_all_names_starting_with_m()
    make_all_males_age_30()
