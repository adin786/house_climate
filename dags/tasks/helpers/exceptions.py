class MissingZone(Exception):
    """Raise if no matching zone found"""

class IncompleteDataDuration(Exception):
    """Raise if less than 24 hours in the API data"""    