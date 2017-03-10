from sqlalchemy import Column, Integer, Text, BigInteger, UniqueConstraint, DateTime, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData

Base = declarative_base(metadata=MetaData(schema='stat_compiled'))


class CoverageStartEndNetworks(Base):
    __tablename__ = 'coverage_start_end_networks'

    region_id = Column(Text(), primary_key=True, nullable=False)
    start_network_id = Column(Text(), primary_key=True, nullable=False)
    start_network_name = Column(Text(), primary_key=False, nullable=False)
    end_network_id = Column(Text(), primary_key=True, nullable=False)
    end_network_name = Column(Text(), primary_key=False, nullable=False)
    request_date = Column(DateTime(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)


class CoverageJourneys(Base):
    __tablename__ = 'coverage_journeys'

    request_date = Column(DateTime(), primary_key=True, nullable=False)
    region_id = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)


class CoverageJourneysRequestParams(Base):
    __tablename__ = 'coverage_journeys_requests_params'

    request_date = Column(DateTime(), primary_key=True, nullable=False)
    region_id = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb_wheelchair = Column(BigInteger(), primary_key=False, nullable=False)


class CoverageJourneysTransfers(Base):
    __tablename__ = 'coverage_journeys_transfers'

    request_date = Column(DateTime(), primary_key=True, nullable=False)
    region_id = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb_transfers = Column(BigInteger(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)


class CoverageModes(Base):
    __tablename__ = 'coverage_modes'

    request_date = Column(DateTime(), primary_key=True, nullable=False)
    region_id = Column(Text(), primary_key=True, nullable=False)
    type = Column(Text(), primary_key=True, nullable=False)
    mode = Column(Text(), primary_key=True, nullable=False)
    commercial_mode_id = Column(Text(), primary_key=True, nullable=False)
    commercial_mode_name = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)


class CoverageNetworks(Base):
    __tablename__ = 'coverage_networks'

    request_date = Column(DateTime(), primary_key=True, nullable=False)
    region_id = Column(Text(), primary_key=True, nullable=False)
    network_id = Column(Text(), primary_key=True, nullable=False)
    network_name = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)


class CoverageStopAreas(Base):
    __tablename__ = 'coverage_stop_areas'

    request_date = Column(DateTime(), primary_key=True, nullable=False)
    region_id = Column(Text(), primary_key=True, nullable=False)
    stop_area_id = Column(Text(), primary_key=True, nullable=False)
    stop_area_name = Column(Text(), primary_key=True, nullable=False)
    city_id = Column(Text(), primary_key=True, nullable=False)
    city_name = Column(Text(), primary_key=True, nullable=False)
    city_insee = Column(Text(), primary_key=True, nullable=False)
    department_code = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)


class ErrorStats(Base):
    __tablename__ = 'error_stats'

    region_id = Column(Text(), primary_key=True, nullable=False)
    api = Column(Text(), primary_key=True, nullable=False)
    user_id = Column(Integer(), primary_key=True, nullable=False)
    app_name = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    request_date = Column(DateTime(), primary_key=True, nullable=False)
    err_id = Column(Text(), primary_key=True, nullable=False)
    nb_req = Column(BigInteger(), primary_key=False, nullable=False)
    nb_without_journey = Column(BigInteger(), primary_key=False, nullable=False)


class RequestsCalls(Base):
    __tablename__ = 'requests_calls'

    region_id = Column(Text(), primary_key=True, nullable=False)
    api = Column(Text(), primary_key=True, nullable=False)
    user_id = Column(Integer(), primary_key=True, nullable=False)
    app_name = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    request_date = Column(DateTime(), primary_key=True, nullable=False)
    end_point_id = Column(Integer(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)
    nb_without_journey = Column(BigInteger(), primary_key=False, nullable=False)
    object_count = Column(BigInteger(), primary_key=False, nullable=False)


class TokenStats(Base):
    __tablename__ = 'token_stats'

    token = Column(Text(), primary_key=True, nullable=False)
    request_date = Column(DateTime(), primary_key=True, nullable=False)
    nb_req = Column(BigInteger(), primary_key=False, nullable=False)

class CoverageLines(Base):
    __tablename__ = 'coverage_lines'

    request_date = Column(DateTime(), primary_key=True, nullable=False)
    region_id = Column(Text(), primary_key=True, nullable=False)
    type = Column(Text(), primary_key=True, nullable=False)
    line_id = Column(Text(), primary_key=True, nullable=False)
    line_code = Column(Text(), primary_key=True, nullable=False)
    network_id = Column(Text(), primary_key=True, nullable=False)
    network_name = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)

class Users(Base):
    __tablename__ = 'users'

    id = Column(Text(), primary_key=True, nullable=False)
    user_name = Column(Text(), primary_key=False, nullable=False)
    date_first_request = Column(DateTime(), primary_key=False, nullable=False)


class CoverageAnticipation(Base):
    __tablename__ = 'coverage_journey_anticipations'

    region_id = Column(Text(), primary_key=True, nullable=False)
    is_internal_call = Column(SmallInteger(), primary_key=True, nullable=False)
    request_date = Column(DateTime(), primary_key=True, nullable=False)
    difference = Column(Integer(), primary_key=True, nullable=False)
    nb = Column(BigInteger(), primary_key=False, nullable=False)

