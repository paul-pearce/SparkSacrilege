def returnValue(value):
    return value

def returnTrue():
    return returnValue(True)
def addTrue(s):
    return s + (returnTrue(),)
