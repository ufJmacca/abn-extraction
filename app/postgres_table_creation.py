from typing import List, Optional
from sqlalchemy import String, Integer, BigInteger, ForeignKey, Date, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class ABN(Base):
    __tablename__ = 'abn'

    abn: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    abn_status: Mapped[str] = mapped_column(String(3))
    abn_status_from_date: Mapped[Date] = mapped_column(Date)

    entity_type_indicator: Mapped[str] = mapped_column(String(3))
    entity_type_text: Mapped[str] = mapped_column(String)

    main_entity: Mapped[Optional["MainEntity"]] = relationship(back_populates="abn", cascade="all, delete-orphan")
    legal_entity: Mapped[Optional["LegalEntity"]] = relationship(back_populates="abn", cascade="all, delete-orphan")

    asic_number: Mapped[Optional["ASICNumber"]] = relationship(back_populates="abn", cascade="all, delete-orphan")
    gst: Mapped[Optional["GST"]] = relationship(back_populates="abn", cascade="all, delete-orphan")
    dgr: Mapped[Optional[List["DGR"]]] = relationship(back_populates="abn", cascade="all, delete-orphan")
    other_entity: Mapped[Optional[List["OtherEntity"]]] = relationship(back_populates="abn", cascade="all, delete-orphan")

class MainEntity(Base):
    __tablename__ = 'main_entity'

    abn_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("abn.abn"), primary_key=True)
    main_entity_type: Mapped[str] = mapped_column(String[3])
    main_entity_name: Mapped[str] = mapped_column(String)
    address_state: Mapped[str] = mapped_column(String)
    address_postcode: Mapped[int] = mapped_column(Integer)
    
    abn: Mapped["ABN"] = relationship(back_populates="main_entity")

class LegalEntity(Base):
    __tablename__ = 'legal_entity'

    abn_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("abn.abn"), primary_key=True)
    legal_entity_type: Mapped[str] = mapped_column(String[3])
    legal_entity_name: Mapped[str] = mapped_column(String)
    address_state: Mapped[str] = mapped_column(String)
    address_postcode: Mapped[int] = mapped_column(Integer)
    
    abn: Mapped["ABN"] = relationship(back_populates="legal_entity")

class ASICNumber(Base):
    __tablename__ = 'asic_number'

    abn_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("abn.abn"), primary_key=True)
    asic_number: Mapped[str] = mapped_column(String)
    asic_type: Mapped[str] = mapped_column(String)

    abn: Mapped["ABN"] = relationship(back_populates="asic_number")

class GST(Base):
    __tablename__ = 'gst'

    abn_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("abn.abn"), primary_key=True, )
    status: Mapped[str] = mapped_column(String)
    status_from_date: Mapped[Date] = mapped_column(Date)

    abn: Mapped["ABN"] = relationship(back_populates="gst")

class DGR(Base):
    __tablename__ = 'dgr'

    abn_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("abn.abn"), primary_key=True)
    status_from_date: Mapped[Date] = mapped_column(Date, primary_key=True)
    name: Mapped[str] = mapped_column(String, primary_key=True)

    abn: Mapped["ABN"] = relationship(back_populates="dgr")

class OtherEntity(Base):
    __tablename__ = 'other_entity'

    abn_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("abn.abn"), primary_key=True)
    other_entity_type: Mapped[str] = mapped_column(String[3], primary_key=True)
    other_entity_name: Mapped[str] = mapped_column(String, primary_key=True)

    abn: Mapped["ABN"] = relationship(back_populates="other_entity")

if __name__ == '__main__':
    engine = create_engine('postgresql://postgres:example_password@abn-db-1/abn')

    try:
        with engine.connect():
            Base.metadata.create_all(engine)
    except Exception as e:
        print(f"Connection error: {e}")