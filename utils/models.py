"""
Model mapping for All Databases
Setup notes:
https://docs.sqlalchemy.org/en/13/orm/tutorial.html
"""
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from sqlalchemy import \
    (Column, Integer, String, ForeignKey, DateTime, Float, Binary, Text, Sequence)
from sqlalchemy.orm import relationship

import logging
mlog = logging.getLogger(__name__)


############################
### Validation Functions ###
############################

# check_exists is here to help prevent circular imports

def check_exists(model, id_column, session, record):
    # Make command to evaluate
    eval_string = "session.query({}).filter_by({}=record['{}']).scalar()".\
                    format(model, id_column, id_column)
    # Evaluate and return result
    return eval(eval_string)

########################
### Helper Functions ###
########################

def get_model_info(model_class):
    model_dir = dir(model_class)
    info = {
        'tablename': model_class.__tablename__,
        'columns': [x for x in model_dir if not x.startswith('_')]
    }
    return info

def generate_add_object(model, record):
    # Make command to evaluate
    eval_string = "{}(**record)".format(model)
    # Evaluate and return result
    return eval(eval_string)


def update_record(model, id_column, record, session):
    # Make command to evaluate
    eval_string = "session.query({}).filter_by({}=record['{}']).update(record)".\
                    format(model, id_column, id_column)
    # Evaluate and return result
    return eval(eval_string)


#################
###Data Models###
#################

class MasterPersonLong(Base):
    __tablename__ = 'master_person_long'

    mpi = Column(String, primary_key=True)
    field = Column(String, primary_key=True)
    value = Column(String)
    score = Column(Float)
    guid = Column(String, primary_key=True)
    # Composite primary key for table

"""
MODEL EXAMPLES
    Full relational examples using Base for manual description of sqlalchemy models.
    


class Variable(Base):
    __tablename__ = 'variable'

    variable_id = Column(String, primary_key=True)
    name = Column(String)
    tablename = Column(String)
    institution = Column(String, ForeignKey('institution.institution_id'))
    udrc_name = Column(String)
    alias = Column(String)
    first_date = Column(DateTime)
    last_date = Column(DateTime)

    _institution = relationship('Institution', back_populates='_variable')
    _nlp = relationship('VariableNLP', uselist=False,
                            back_populates='_variable')
    _description = relationship('VariableDescription', uselist=False,
                                    back_populates='_variable')


class VariableDescription(Base):
    __tablename__ = "variable_description"

    variable_id = Column(String, ForeignKey('variable.variable_id'), primary_key=True)
    variable_type = Column(String)
    description = Column(Text)

    _variable = relationship('Variable', back_populates='_description')
    # Break down into numeric and categorical/string
    # max, min, mean, median, mode, count, missing, stddev, skewness, kurtosis
    # most common, least common, number unique, count, missing


# rep_nlp_seq = Sequence('seq_nlp_seq')  # PostgreSQL, Oracle only
"""

################################
### NOSQL (MongoDB) Template ###
################################


mongo_model = {
    "mpi": str,
    "sources": [
        {
            "guid": int,
            "fields": [
                {
                    "fieldname": str,
                    "value": str,
                }
            ],
            "score": float,
        }
    ]
}


## NoSQL Utility ##
class Validator():
    def __init__(self, expected=None, query=None):
        self.expected = expected
        self.query = query

    def _check_expected(self, x):
        if self.expected == 'any':
            return x is not None 
        return x == self.expected

    def _query(self, data):
        def _int(x):
            try:
                return int(x)
            except:
                return x
        val = data
        for cond in self.query.split('.'):
            try:
                val = val[_int(cond)]
            except Exception as e:
                mlog.error(f"Failed query: {val}, {cond}, {e}")
        return type(val)  # type validation

    def test(self, data):
        result = self._check_expected(self._query(data))
        if result == False:
            mlog.error(f"failed: { self.query}")
        return result


# Define model validators for each field
validators = (
    Validator(query='mpi', expected='any'),
    Validator(query='sources', expected=list),
    Validator(query='sources.0.guid', expected=int),
    Validator(query='sources.0.fields', expected=list),
    Validator(query='sources.0.fields.0.fieldname', expected=str),
    Validator(query='sources.0.fields.0.value', expected='any'),
    Validator(query='sources.0.score', expected=float)
)


def validate_model(data, validators=validators):
    
    def _run_validator(validator, data=data):
        return validator.test(data)

    # mlog.debug(f"{list(map(_run_validator, validators))}, {len(validators)}")
    return sum(list(map(_run_validator, validators))) == len(validators)


# Build a serializer to convert from raw vector

class NoSQLSerializer():

    def __init__(self, validator=validate_model):
        self.validator = validate_model


    def _validate_doc(self, document):
        if self.validator(document):
            return document
        else:
            raise ValueError(f'Invalid document created during serialization.  Check:\n{document}')

    def _check_raw(self, raw):
        assert 'mpi' in raw, 'Cannot marshal. Missing MPI in expected key group.'
        assert 'guid' in raw, 'Cannot marshal.  Missing GUID in expected key group.'
        return raw


    def _marshal(self, raw):
        def _is_field(x):
            return x not in ['mpi', 'guid', 'score']

        mpi = raw['mpi']
        guid = raw['guid']
        if 'score' in raw:
            score = raw['score']
        else:
            score = 0.00
        return {
            'mpi': mpi,
            'sources': [
                {
                    'guid': guid,
                    'score': score,
                    'fields': [
                        {'fieldname': key, 'value': raw[key]} for key in raw if _is_field(key)]
                }
            ]
        }


    def __call__(self, raw):
        return self._validate_doc(
            self._marshal(self._check_raw(raw))
        ) 