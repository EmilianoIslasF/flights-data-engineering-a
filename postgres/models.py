from __future__ import annotations

from typing import List, Optional

from sqlalchemy import Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Airline(Base):
    __tablename__ = "airlines"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airline: Mapped[str] = mapped_column(String(255), nullable=False)

    flights: Mapped[List["Flight"]] = relationship(
        back_populates="airline_rel"
    )


class Airport(Base):
    __tablename__ = "airports"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airport: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    departures: Mapped[List["Flight"]] = relationship(
        foreign_keys="Flight.origin_airport",
        back_populates="origin_airport_rel",
    )
    arrivals: Mapped[List["Flight"]] = relationship(
        foreign_keys="Flight.destination_airport",
        back_populates="destination_airport_rel",
    )


class Flight(Base):
    __tablename__ = "flights"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    day: Mapped[int] = mapped_column(Integer, nullable=False)

    airline: Mapped[str] = mapped_column(
        ForeignKey("airlines.iata_code"),
        nullable=False,
    )
    origin_airport: Mapped[str] = mapped_column(
        ForeignKey("airports.iata_code"),
        nullable=False,
    )
    destination_airport: Mapped[str] = mapped_column(
        ForeignKey("airports.iata_code"),
        nullable=False,
    )

    departure_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    arrival_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cancelled: Mapped[int] = mapped_column(Integer, nullable=False)
    cancellation_reason: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    distance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    air_system_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    airline_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weather_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    late_aircraft_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    security_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    airline_rel: Mapped["Airline"] = relationship(
        back_populates="flights"
    )
    origin_airport_rel: Mapped["Airport"] = relationship(
        foreign_keys=[origin_airport],
        back_populates="departures",
    )
    destination_airport_rel: Mapped["Airport"] = relationship(
        foreign_keys=[destination_airport],
        back_populates="arrivals",
    )
