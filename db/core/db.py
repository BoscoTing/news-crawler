from sqlmodel import Session, create_engine

from config import settings

engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI))


def init_db(session: Session) -> None:
    import models
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)
