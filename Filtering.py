"""
This is the file for the filtering function. The general idea is to make the filter function as general as possible, so that it's able to filter as many options as possible.

To acheive this, the filter takes the conditions that we want as argument. It's form is as follows:

{
"argument_one": [data_position, "operator", comparative_value],
"argument_two": [data_position, "operator", comparative_value],
.
.
.
"argument_N": [data_position, "distance", lat/long, amount]
}

Notice what we feed in is a dictionary. The arguments can be completely arbitrary strings.
The data position can take multiple integer values depending on the operator.
The operator must be one of the listed operators programmed into the function. An arbitrary number of these could be added to our function depending on our future needs.
The comparative_value must be what we compare against (latitude > _12_). Generally an integer or float.
"""

def conditions(conditions):
    '''
    Takes a row in, and a dictionary of conditions that we'll filter against.
    We must manually construct all operator conditions.

    The ones that i've included are the following:

    "gt": greater than
    "lt": less than
    "lte": less than or equal to
    "gte": greater than or equal to
    "distance": distance from presumably lat/long coordinates and input lat/long coordinates within some amount.


    '''
    def actual_function(row):
        for key in conditions:
            position, operator, comparison = conditions[0], conditions[1], conditions[2]
            if operator == "gt":
                if not row[position] > comparison:
                    return False
            elif operator == "lt":
                if not row[position] < comparison:
                    return False
            elif operator == "gte":
                if not row[position] >= comparison:
                    return False
            elif operator == "distance":
                vector = np.array([row[x] for x in in position])
                distance = np.dot(vector - np.array(comparison), vector - np.array(comparison))
                if not distance < conditions[3]:
                    return False
        return True
    return actual_function




'''
This is essentially a refactored version of the above code:

For this one, the key (called argument above) is actually the data, the value will be the operation applied.

exammple:
{
(1,3):"gt",
(2,6):"lt",
((3,4),(-75,76),0.001):"dist"
}
'''

first = sc.whatever
second = first.filter(closed(dictionary_of_conditions))

def closed(args):
    """
    The closure function is kinda a formality so that we can pass the conditions dictionary into the filter function.

    This function takes as argument a dictionary of conditions. The keys will be the appropriately formatted arguments to be passed into the operator (which will be the value)
    Returns the actual function that the filter will utilize.
    """
    def distance(row, value):
        import numpy as np
        """ The value should look like: ((Position_Vector),(Comparison_Vector), Amount)
        Returns True is the distance between the vectors is not less than the amount given """
        location = np.array([row[x] for x in value[0]])
        bar_location = np.array(value[1])
        difference = location-bar_location
        amount = value[2]
        return not np.dot(difference,difference) < amount
    def distance_miles(row, value):
        from geopy import distance
        """
        The value should look like: ((Position_of_lat/long), (Comparison Lat/Long), Mile_Distance)
        Make sure it's actually latitude then longitude for accurate distances.

        Example of usage with the filter function:
        RDD.filter(closed({ ((4,5), (41.49008, -71.312796), 1000) : "miles" }))
        This will remove any values more than 1000 miles away from that position.



        Returns True if the distance between the vectors is not less than or equal to the Mile Distance given.
        (returning true makes sure that the row is actually FALSE in the filter)
        """
        pickup_location = tuple([row[x] for x in value[0]])
        comparison_location = value[1]
        distance = distance.distance(pickup_location, comparison_location).miles
        return not distance <= value[2]

    functions = {
    "gt": lambda row, value: not row[value[0]] > value[1], # (1,3):"gt" if data in position 1 is not greater than 3, then we drop the row from the data (returns true, so that the if statement can return "false")
    "lt": lambda row, value: not row[value[0]] < value[1], # (1,3):"lt" if data in position 1 is not less than 3, then we drop the row from the data.
    "gte": lambda row, value: not row[value[0]] >= value[1],
    "lte": lambda row, value: not row[value[0]] <= value[1],
    "eq": lambda row, value: not row[value[0]] == value[1],  # (1, 3): "eq" if data in position 1 is equal to 3 then we drop the row from the data.
    "dist": distance,
    "miles": distance_miles
    }
    def actual_function(row):
        for val in args:
            if functions[args[val]](row, val):
                return False
        return True
    return actual_function
