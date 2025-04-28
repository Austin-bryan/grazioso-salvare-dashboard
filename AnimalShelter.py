from pymongo import MongoClient
from bson.objectid import ObjectId

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """
    def __init__(self, username, password):
        from pymongo import MongoClient
        import certifi

        # Setup MongoDB connection
        USER = username
        PASS = password
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 33688
        DB   = 'AAC'
        COL  = 'animals'
        #
        # Initialize Connection
        #
        self.client     = MongoClient(f'mongodb://{USER}:{PASS}@{HOST}:{PORT}')
        self.database   = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]
        
    def filter_dogs(self, rescue_type):
        # Limit valid types
        valid_types = {
            "Water Rescue",
            "Mountain Rescue",
            "Disaster Rescue",
            "Reset"
        }

        # Enforce string
        if not isinstance(rescue_type, str):
            raise ValueError("rescue_type must be a string.")

        # Normalize input (capitalization + strip spaces)
        rescue_type = rescue_type.strip().title()

        # Return no filter if the rescue type is invalid
        if rescue_type not in valid_types:
            return self.read({})

        query = {}

        if rescue_type == "Water Rescue":
            # Using all breed name variants found in dataset for water rescue suitability
            query = {
                "animal_type": "Dog",
                "breed": {
                    "$in": [
                        "Labrador Retriever",
                        "Labrador Retriever Mix",
                        "Labrador Retriever/Chesa Bay Retr",
                        "Chesa Bay Retr Mix",
                        "Newfoundland",
                        "Newfoundland Mix",
                        "Newfoundland/Labrador Retriever"
                    ]
                },
                "sex_upon_outcome": "Intact Female",
                "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156}
            }

        elif rescue_type == "Mountain Rescue":
            # Using all breed name variants found in dataset for mountain rescue suitability
            query = {
                "animal_type": "Dog",
                "breed": {
                    "$in": [
                        "German Shepherd",
                        "German Shepherd Mix",
                        "Alaskan Malamute",
                        "Alaskan Malamute Mix",
                        "Old English Sheepdog",
                        "Siberian Husky",
                        "Siberian Husky Mix",
                        "Rottweiler",
                        "Rottweiler Mix"
                    ]
                },
                "sex_upon_outcome": "Intact Male",
                "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156}
            }

        elif rescue_type == "Disaster Rescue":
            # Using all breed name variants found in dataset for disaster rescue suitability
            query = {
                "animal_type": "Dog",
                "breed": {
                    "$in": [
                        "Doberman Pinsch",
                        "Doberman Pinsch Mix",
                        "German Shepherd",
                        "German Shepherd Mix",
                        "Golden Retriever",
                        "Golden Retriever Mix",
                        "Bloodhound",
                        "Bloodhound Mix",
                        "Rottweiler",
                        "Rottweiler Mix"
                    ]
                },
                "sex_upon_outcome": "Intact Male",
                "age_upon_outcome_in_weeks": {"$gte": 20, "$lte": 300}
            }

        elif rescue_type == "Reset":
            query = {}  # Unfiltered view (show all data)

        return self.read(query)

    def create(self, data):
        """ Insert a document into the database """
        if data is not None:
            try:
                # Can successfully insert
                self.collection.insert_one(data)
                return True
            except Exception as e:
                # An error occurred while inserting
                print(f"An error occurred: {e}")
                return False
        else:
            # data was None so throw exception
            raise Exception("Nothing to save, because data parameter is empty")

    def read(self, query):
        """ Read documents from the database """
        try:
            # Return the result found from the database
            return list(self.collection.find(query))
        except Exception as e:
            # Return an empty list if an exception was found
            print(f"An error occurred: {e}")
            return []
        
    def update(self, query, new_values):
        """ Update document(s) in the database matching query with new values """
        if not isinstance(query, dict) or not isinstance(new_values, dict):
            # Forces both query and new values to be dictionaries by throwing an exception if one isnt
            raise TypeError("Both query and new_values must be dictionaries.")
        try:
            # Update documents that match the query to the new values
            result = self.collection.update_many(query, {"$set": new_values})
            return result.modified_count
        except Exception as e:
            # If an error occurs, print the error then return 0
            print(f"An error occurred during update: {e}")
            return 0
        
    def delete(self, query):
        """ Delete document(s) from the database matching query """
        if not isinstance(query, dict):
            # Forces query to be dictionaries by throwing an exception if it isnt
            raise TypeError("Query must be a dictionary.")
        try:
            # Delete documents that match the document
            result = self.collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            # If an error occurs, print the error then return 0
            print(f"An error occurred during delete: {e}")
            return 0

