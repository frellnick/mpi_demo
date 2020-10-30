"""
Model mapping for DataDictionary
Setup notes:
https://docs.sqlalchemy.org/en/13/orm/tutorial.html
"""
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from sqlalchemy import \
    (Column, Integer, String, ForeignKey, DateTime, Float, Binary, Text, Sequence)
from sqlalchemy.orm import relationship

#########################
### Database MetaData ###
#########################

# Update this with any changes to underlying model.
#   Query functions use as a lookup for building routes

classes_tables_keys = {
    'Institution': {
        'tablename':'institution',
        'primary_key': 'institution_id',
        'parent_key': 'institution_id',  # Column is foreign key for other table
        'foreign_keys': None,
    },
    'Report': {
        'tablename': 'report',
        'primary_key': 'report_id',
        'parent_key': 'report_id',
        'foreign_keys': 'institution_id',
    },
    'Variable': {
        'tablename': 'variable',
        'primary_key': 'variable_id',
        'parent_key': 'variable_id',
        'foreign_keys': None,
    },
    'VariableDescription': {
        'tablename': 'variable_description',
        'primary_key': 'variable_id',
        'parent_key': None,
        'foreign_keys': 'variable_id',
    },
    'ReportNLP': {
        'tablename': 'report_nlp',
        'primary_key': 'report_id',
        'parent_key': None,
        'foreign_keys': 'report_id'
    },
    'VariableNLP': {
        'tablename': 'variable_nlp',
        'primary_key': 'variable_id',
        'parent_key': None,
        'foreign_keys': 'variable_id'
    }
}

# Search function
def search_table_metadata(**kwargs):
    # Return appropriate model, self id column, and necessary foreign key to check)
    active_model_metadata = None
    
    keyword = list(kwargs.keys())[0]
    value = kwargs[keyword]
    
    for model in classes_tables_keys.keys():
        if classes_tables_keys[model][keyword] == value:
            active_model_metadata = classes_tables_keys[model]
            active_model = model
    
    if active_model_metadata is None: 
        print('No model found with matching {} = {}'.format(keyword, value))
        raise ValueError

    active_model_metadata.update(model = active_model)
    return active_model_metadata


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