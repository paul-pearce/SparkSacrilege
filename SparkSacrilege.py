import types
import dill

class SparkSacrilege:
    """ 
    Wrapper class for python imports to avoid having to restart jupyter
    notebooks for changes in the import. The imports are sent dynamically
    to the cluster instead of via --py-files.

    Example usecase: You have several helper functions you use in pyspark
    RDDs or UDFs. These are in library.py. Normally you'd have to send this
    code to the cluster via --py-files and any changes to the code require
    restarting to jupyter notebook instance. This can be frustrating.

    With SparkSacrilege the instantiated object houses the import and 
    the code from the library is dynamically sent to workers, rather than
    supplied by --py-files.
    """
    def __init__(self, className):
        self.dispatch = self.generateLambda(className)

    def generateLambda(self, className):
        # Please address all complaints and calls to retract my degree
        # to Berkeley EECS.
        env = {}
        if not className.endswith(".py"):
            className = "%s.py" % className
        execfile(className, env)
        
        return lambda key: env[key]

    def __getattr__(self, key):
        return self.dispatch(key)

    def __getstate__(self):
        state = {}
        state["code"] = self.dispatch.func_code
        state["name"] = self.dispatch.func_name
        state["defs"] = self.dispatch.func_defaults
        state["closure"] = self.dispatch.func_closure
        return state

    def __setstate__(self, state):
        self.dispatch = types.FunctionType(state["code"], {}, state["name"], 
                            state["defs"], state["closure"])
