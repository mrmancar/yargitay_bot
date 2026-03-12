from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Case(Base):
    __tablename__ = "cases"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    daire: Mapped[str | None] = mapped_column(Text, nullable=True)
    esas_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    karar_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    karar_tarihi_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    aranan_kelime: Mapped[str | None] = mapped_column(Text, nullable=True)

    source_list_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    detail_fetched: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    parsed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class CaseDetail(Base):
    __tablename__ = "case_details"

    case_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("cases.id"),
        primary_key=True
    )
    raw_response: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class ParsedCaseDocument(Base):
    __tablename__ = "parsed_case_documents"

    case_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("cases.id"),
        primary_key=True
    )
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    daire_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    esas_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    karar_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    mahkemesi: Mapped[str | None] = mapped_column(Text, nullable=True)
    tarihi: Mapped[str | None] = mapped_column(Text, nullable=True)
    numarasi: Mapped[str | None] = mapped_column(Text, nullable=True)
    ictihat_metni: Mapped[str | None] = mapped_column(Text, nullable=True)
    clean_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    parsed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)