import os
from datetime import datetime, date
from typing import List, Optional, Any, Dict

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Float,
    ForeignKey,
    Date,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
from sqlalchemy.sql import func

# -----------------------------
# Database setup (MySQL preferred)
# -----------------------------
SQL_DATABASE_URL = (
    os.getenv("MYSQL_URL")
    or os.getenv("DATABASE_URL")
    or "sqlite+pysqlite:///./app.db"
)

# Ensure sqlite works without thread issues in dev
connect_args = {}
if SQL_DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(SQL_DATABASE_URL, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# -----------------------------
# ORM Models
# -----------------------------
class Supplier(Base):
    __tablename__ = "suppliers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False)
    contact = Column(String(255))
    email = Column(String(255))
    phone = Column(String(50))
    address = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    purchase_orders = relationship("PurchaseOrder", back_populates="supplier")


class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String(64), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    uom = Column(String(32), default="pcs")
    description = Column(Text)
    min_stock = Column(Float, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    pr_items = relationship("PurchaseRequisitionItem", back_populates="item")
    po_items = relationship("PurchaseOrderItem", back_populates="item")
    grn_items = relationship("GRNItem", back_populates="item")
    mr_items = relationship("MaterialRequestItem", back_populates="item")
    stock_txns = relationship("StockTransaction", back_populates="item")


class PurchaseRequisition(Base):
    __tablename__ = "purchase_requisitions"
    id = Column(Integer, primary_key=True, index=True)
    pr_number = Column(String(64), unique=True, index=True, nullable=False)
    requested_by = Column(String(255))
    department = Column(String(255))
    status = Column(String(32), default="OPEN")  # OPEN, APPROVED, CLOSED, CANCELLED
    requested_date = Column(Date, default=date.today)
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    items = relationship("PurchaseRequisitionItem", back_populates="pr", cascade="all, delete-orphan")
    extra = relationship("PRExtra", back_populates="pr", uselist=False, cascade="all, delete-orphan")


class PRExtra(Base):
    __tablename__ = "pr_extra"
    id = Column(Integer, primary_key=True)
    pr_id = Column(Integer, ForeignKey("purchase_requisitions.id", ondelete="CASCADE"), unique=True, nullable=False)
    # Business keys and carry-forward fields
    customer_name = Column(String(255), nullable=False)
    customer_ref_no = Column(String(128), nullable=False)
    supplier_name = Column(String(255))
    supplier_address = Column(Text)
    tax_code = Column(String(32))
    tax_percent = Column(Float, default=0)
    # Approvals
    approval_status = Column(String(32), default="PENDING")  # PENDING, APPROVED, REJECTED
    approval_level = Column(Integer, default=0)
    approval_flow = Column(Text)  # JSON string with stages

    pr = relationship("PurchaseRequisition", back_populates="extra")

    __table_args__ = (
        UniqueConstraint("customer_name", "customer_ref_no", name="uq_customer_ref"),
    )


class PurchaseRequisitionItem(Base):
    __tablename__ = "purchase_requisition_items"
    id = Column(Integer, primary_key=True)
    pr_id = Column(Integer, ForeignKey("purchase_requisitions.id", ondelete="CASCADE"), nullable=False)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    qty = Column(Float, nullable=False)
    target_price = Column(Float, default=0)
    notes = Column(Text)

    pr = relationship("PurchaseRequisition", back_populates="items")
    item = relationship("Item", back_populates="pr_items")
    extra = relationship("PRItemExtra", back_populates="pr_item", uselist=False, cascade="all, delete-orphan")


class PRItemExtra(Base):
    __tablename__ = "pr_item_extra"
    id = Column(Integer, primary_key=True)
    pr_item_id = Column(Integer, ForeignKey("purchase_requisition_items.id", ondelete="CASCADE"), unique=True, nullable=False)
    qty_bags = Column(Float, default=0)
    qty_kgs = Column(Float, default=0)
    unit_price = Column(Float, default=0)
    line_tax_percent = Column(Float)

    pr_item = relationship("PurchaseRequisitionItem", back_populates="extra")


class PurchaseOrder(Base):
    __tablename__ = "purchase_orders"
    id = Column(Integer, primary_key=True, index=True)
    po_number = Column(String(64), unique=True, index=True, nullable=False)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"), nullable=False)
    pr_id = Column(Integer, ForeignKey("purchase_requisitions.id"), nullable=True)
    order_date = Column(Date, default=date.today)
    status = Column(String(32), default="OPEN")  # OPEN, PARTIAL, RECEIVED, CLOSED, CANCELLED
    notes = Column(Text)

    supplier = relationship("Supplier", back_populates="purchase_orders")
    pr = relationship("PurchaseRequisition")
    items = relationship("PurchaseOrderItem", back_populates="po", cascade="all, delete-orphan")
    grns = relationship("GRN", back_populates="po")
    extra = relationship("POExtra", back_populates="po", uselist=False, cascade="all, delete-orphan")


class POExtra(Base):
    __tablename__ = "po_extra"
    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey("purchase_orders.id", ondelete="CASCADE"), unique=True, nullable=False)
    tax_code = Column(String(32))
    tax_percent = Column(Float, default=0)
    approval_status = Column(String(32), default="PENDING")
    approval_level = Column(Integer, default=0)
    approval_flow = Column(Text)

    po = relationship("PurchaseOrder", back_populates="extra")


class PurchaseOrderItem(Base):
    __tablename__ = "purchase_order_items"
    id = Column(Integer, primary_key=True)
    po_id = Column(Integer, ForeignKey("purchase_orders.id", ondelete="CASCADE"), nullable=False)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    qty_ordered = Column(Float, nullable=False)
    unit_price = Column(Float, nullable=False)

    po = relationship("PurchaseOrder", back_populates="items")
    item = relationship("Item", back_populates="po_items")


class GRN(Base):
    __tablename__ = "grns"
    id = Column(Integer, primary_key=True, index=True)
    grn_number = Column(String(64), unique=True, index=True, nullable=False)
    po_id = Column(Integer, ForeignKey("purchase_orders.id"), nullable=False)
    received_date = Column(Date, default=date.today)
    status = Column(String(32), default="RECEIVED")  # RECEIVED, PARTIAL, CLOSED
    notes = Column(Text)

    po = relationship("PurchaseOrder", back_populates="grns")
    items = relationship("GRNItem", back_populates="grn", cascade="all, delete-orphan")


class GRNItem(Base):
    __tablename__ = "grn_items"
    id = Column(Integer, primary_key=True)
    grn_id = Column(Integer, ForeignKey("grns.id", ondelete="CASCADE"), nullable=False)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    qty_received = Column(Float, nullable=False)

    grn = relationship("GRN", back_populates="items")
    item = relationship("Item", back_populates="grn_items")


class MaterialRequest(Base):
    __tablename__ = "material_requests"
    id = Column(Integer, primary_key=True, index=True)
    mr_number = Column(String(64), unique=True, index=True, nullable=False)
    department = Column(String(255))
    requested_by = Column(String(255))
    requested_date = Column(Date, default=date.today)
    status = Column(String(32), default="OPEN")  # OPEN, ISSUED, PARTIAL, CLOSED, CANCELLED
    notes = Column(Text)

    items = relationship("MaterialRequestItem", back_populates="mr", cascade="all, delete-orphan")


class MaterialRequestItem(Base):
    __tablename__ = "material_request_items"
    id = Column(Integer, primary_key=True)
    mr_id = Column(Integer, ForeignKey("material_requests.id", ondelete="CASCADE"), nullable=False)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    qty_requested = Column(Float, nullable=False)
    qty_issued = Column(Float, default=0)

    mr = relationship("MaterialRequest", back_populates="items")
    item = relationship("Item", back_populates="mr_items")


class StockTransaction(Base):
    __tablename__ = "stock_transactions"
    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    txn_date = Column(DateTime, default=datetime.utcnow)
    txn_type = Column(String(32), nullable=False)  # PO_RECEIPT, ISSUE, ADJUST
    ref_type = Column(String(32))  # PO, GRN, MR, ADJUST
    ref_id = Column(Integer)
    qty_delta = Column(Float, nullable=False)
    notes = Column(Text)

    item = relationship("Item", back_populates="stock_txns")


class TaxRate(Base):
    __tablename__ = "tax_rates"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(32), index=True, nullable=False)  # e.g., VAT, GST
    name = Column(String(128), nullable=False)
    rate_percent = Column(Float, nullable=False)  # 5.0 means 5%
    effective_date = Column(Date, nullable=False, index=True)
    is_active = Column(Integer, default=1)  # 1=true, 0=false
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ApprovalConfig(Base):
    __tablename__ = "approval_config"
    id = Column(Integer, primary_key=True)
    doc_type = Column(String(32), nullable=False)  # e.g., PR, PO
    flow_json = Column(Text, nullable=False)  # JSON string of stages
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("doc_type", name="uq_doc_type"),
    )


# -----------------------------
# Pydantic Schemas
# -----------------------------
class SupplierIn(BaseModel):
    name: str
    contact: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None


class SupplierOut(SupplierIn):
    id: int
    class Config:
        from_attributes = True


class ItemIn(BaseModel):
    sku: str
    name: str
    uom: str = "pcs"
    description: Optional[str] = None
    min_stock: float = 0


class ItemOut(ItemIn):
    id: int
    class Config:
        from_attributes = True


class PRItemIn(BaseModel):
    item_id: int
    # New detailed quantities
    qty_bags: Optional[float] = None
    qty_kgs: Optional[float] = None
    unit_price: Optional[float] = 0
    notes: Optional[str] = None


class PRCreate(BaseModel):
    pr_number: str = Field(..., description="Reference number")
    requested_by: Optional[str] = None
    department: Optional[str] = None
    requested_date: Optional[date] = None
    status: str = "OPEN"
    notes: Optional[str] = None
    # Business keys and header
    customer_name: str
    customer_ref_no: str
    supplier_name: Optional[str] = None
    supplier_address: Optional[str] = None
    tax_code: Optional[str] = None
    items: List[PRItemIn]


class PROutItem(BaseModel):
    item_id: int
    qty_bags: Optional[float] = None
    qty_kgs: Optional[float] = None
    unit_price: Optional[float] = 0
    notes: Optional[str] = None


class PROut(BaseModel):
    id: int
    pr_number: str
    requested_by: Optional[str]
    department: Optional[str]
    requested_date: date
    status: str
    notes: Optional[str]
    customer_name: str
    customer_ref_no: str
    supplier_name: Optional[str]
    supplier_address: Optional[str]
    tax_code: Optional[str]
    tax_percent: Optional[float]
    approval_status: Optional[str]
    approval_level: Optional[int]
    items: List[PROutItem]

    class Config:
        from_attributes = True


class POItemIn(BaseModel):
    item_id: int
    qty_ordered: float
    unit_price: float


class POCreate(BaseModel):
    po_number: str
    supplier_id: Optional[int] = None
    pr_id: Optional[int] = None
    order_date: Optional[date] = None
    status: str = "OPEN"
    notes: Optional[str] = None
    tax_code: Optional[str] = None
    items: List[POItemIn]


class GRNItemIn(BaseModel):
    item_id: int
    qty_received: float


class GRNCreate(BaseModel):
    grn_number: str
    po_id: int
    received_date: Optional[date] = None
    status: str = "RECEIVED"
    notes: Optional[str] = None
    items: List[GRNItemIn]


class MRItemIn(BaseModel):
    item_id: int
    qty_requested: float
    qty_issued: float = 0


class MRCreate(BaseModel):
    mr_number: str
    department: Optional[str] = None
    requested_by: Optional[str] = None
    requested_date: Optional[date] = None
    status: str = "OPEN"
    notes: Optional[str] = None
    items: List[MRItemIn]


class StockTxnOut(BaseModel):
    id: int
    item_id: int
    txn_date: datetime
    txn_type: str
    ref_type: Optional[str]
    ref_id: Optional[int]
    qty_delta: float
    notes: Optional[str]
    class Config:
        from_attributes = True


class TaxRateIn(BaseModel):
    code: str = Field(..., description="Tax code, e.g., VAT, GST")
    name: str
    rate_percent: float = Field(..., ge=0)
    effective_date: date
    is_active: int = 1


class TaxRateOut(TaxRateIn):
    id: int
    class Config:
        from_attributes = True


class ApprovalFlowIn(BaseModel):
    doc_type: str = Field("PR", description="Document type")
    stages: List[Dict[str, Any]] = Field(..., description="List of stages in order, e.g., [{level:1,name:'Manager'}]")


class ApproveAction(BaseModel):
    decision: str = Field(..., pattern="^(approve|reject)$")
    approver: Optional[str] = None
    comments: Optional[str] = None


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Procurement & Inventory API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Initialize DB
Base.metadata.create_all(bind=engine)


# -----------------------------
# Health
# -----------------------------
@app.get("/", tags=["health"])
def root():
    backend = "Running"
    db_type = "mysql" if "mysql" in SQL_DATABASE_URL else ("sqlite" if "sqlite" in SQL_DATABASE_URL else "unknown")
    return {"backend": backend, "db": db_type}


# -----------------------------
# Masters: Suppliers & Items
# -----------------------------
@app.post("/suppliers", response_model=SupplierOut, tags=["masters"])
def create_supplier(data: SupplierIn, db: Session = Depends(get_db)):
    existing = db.query(Supplier).filter(Supplier.name == data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Supplier already exists")
    obj = Supplier(**data.dict())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@app.get("/suppliers", response_model=List[SupplierOut], tags=["masters"])
def list_suppliers(db: Session = Depends(get_db)):
    return db.query(Supplier).order_by(Supplier.name).all()


@app.post("/items", response_model=ItemOut, tags=["masters"])
def create_item(data: ItemIn, db: Session = Depends(get_db)):
    existing = db.query(Item).filter((Item.sku == data.sku) | (Item.name == data.name)).first()
    if existing:
        raise HTTPException(status_code=400, detail="Item with same SKU or name exists")
    obj = Item(**data.dict())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@app.get("/items", response_model=List[ItemOut], tags=["masters"])
def list_items(db: Session = Depends(get_db)):
    return db.query(Item).order_by(Item.name).all()


# -----------------------------
# Taxes (Occasionally adjusted via effective dates)
# -----------------------------
@app.post("/taxes", response_model=TaxRateOut, tags=["taxes"])
def create_tax_rate(data: TaxRateIn, db: Session = Depends(get_db)):
    obj = TaxRate(**data.dict())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@app.get("/taxes", response_model=List[TaxRateOut], tags=["taxes"])
def list_tax_rates(code: Optional[str] = None, active_only: bool = False, db: Session = Depends(get_db)):
    q = db.query(TaxRate)
    if code:
        q = q.filter(TaxRate.code == code)
    if active_only:
        q = q.filter(TaxRate.is_active == 1)
    return q.order_by(TaxRate.code.asc(), TaxRate.effective_date.desc()).all()


@app.get("/taxes/current", tags=["taxes"])
def get_current_tax(code: str = Query(..., description="Tax code, e.g., VAT"), on_date: Optional[date] = None, db: Session = Depends(get_db)):
    d = on_date or date.today()
    row = (
        db.query(TaxRate)
        .filter(TaxRate.code == code, TaxRate.is_active == 1, TaxRate.effective_date <= d)
        .order_by(TaxRate.effective_date.desc(), TaxRate.id.desc())
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="No active tax rate found for the given code and date")
    return {
        "id": row.id,
        "code": row.code,
        "name": row.name,
        "rate_percent": row.rate_percent,
        "effective_date": row.effective_date,
    }


# -----------------------------
# Approval Config (Admin)
# -----------------------------
@app.post("/approvals/pr-flow", tags=["approvals"])
def set_pr_approval_flow(data: ApprovalFlowIn, db: Session = Depends(get_db)):
    if data.doc_type != "PR":
        raise HTTPException(status_code=400, detail="Only PR doc_type supported here")
    row = db.query(ApprovalConfig).filter(ApprovalConfig.doc_type == "PR").first()
    import json
    flow_json = json.dumps(data.stages)
    if row:
        row.flow_json = flow_json
    else:
        row = ApprovalConfig(doc_type="PR", flow_json=flow_json)
        db.add(row)
    db.commit()
    return {"message": "Approval flow set", "stages": data.stages}


@app.get("/approvals/pr-flow", tags=["approvals"])
def get_pr_approval_flow(db: Session = Depends(get_db)):
    row = db.query(ApprovalConfig).filter(ApprovalConfig.doc_type == "PR").first()
    import json
    stages = json.loads(row.flow_json) if row else []
    return {"stages": stages}


@app.post("/approvals/po-flow", tags=["approvals"])
def set_po_approval_flow(data: ApprovalFlowIn, db: Session = Depends(get_db)):
    if data.doc_type != "PO":
        raise HTTPException(status_code=400, detail="Use doc_type=PO for PO flow")
    row = db.query(ApprovalConfig).filter(ApprovalConfig.doc_type == "PO").first()
    import json
    flow_json = json.dumps(data.stages)
    if row:
        row.flow_json = flow_json
    else:
        row = ApprovalConfig(doc_type="PO", flow_json=flow_json)
        db.add(row)
    db.commit()
    return {"message": "Approval flow set", "stages": data.stages}


@app.get("/approvals/po-flow", tags=["approvals"])
def get_po_approval_flow(db: Session = Depends(get_db)):
    row = db.query(ApprovalConfig).filter(ApprovalConfig.doc_type == "PO").first()
    import json
    stages = json.loads(row.flow_json) if row else []
    return {"stages": stages}


# -----------------------------
# Purchase Requisition
# -----------------------------
@app.post("/purchase-requisitions", tags=["purchase_requisition"])
def create_pr(data: PRCreate, db: Session = Depends(get_db)):
    # Mandatory composite business keys
    if not data.customer_name or not data.customer_ref_no:
        raise HTTPException(status_code=400, detail="customer_name and customer_ref_no are required")

    if db.query(PurchaseRequisition).filter(PurchaseRequisition.pr_number == data.pr_number).first():
        raise HTTPException(status_code=400, detail="PR number already exists")

    # Create PR header
    pr = PurchaseRequisition(
        pr_number=data.pr_number,
        requested_by=data.requested_by,
        department=data.department,
        requested_date=data.requested_date or date.today(),
        status=data.status,
        notes=data.notes,
    )
    db.add(pr)
    db.flush()

    # Fetch approval flow (admin configured)
    cfg = db.query(ApprovalConfig).filter(ApprovalConfig.doc_type == "PR").first()
    import json
    stages = []
    if cfg:
        try:
            stages = json.loads(cfg.flow_json)
        except Exception:
            stages = []

    # Determine tax snapshot
    tax_percent = 0.0
    if data.tax_code:
        # find current tax automatically
        today = date.today()
        row = (
            db.query(TaxRate)
            .filter(TaxRate.code == data.tax_code, TaxRate.is_active == 1, TaxRate.effective_date <= today)
            .order_by(TaxRate.effective_date.desc(), TaxRate.id.desc())
            .first()
        )
        if row:
            tax_percent = float(row.rate_percent)

    # Create Extra header info with unique business key constraint
    extra = PRExtra(
        pr_id=pr.id,
        customer_name=data.customer_name,
        customer_ref_no=data.customer_ref_no,
        supplier_name=data.supplier_name,
        supplier_address=data.supplier_address,
        tax_code=data.tax_code,
        tax_percent=tax_percent,
        approval_status="PENDING",
        approval_level=0,
        approval_flow=json.dumps(stages) if stages else json.dumps([]),
    )
    # Enforce uniqueness of customer_name + customer_ref_no
    existing_extra = db.query(PRExtra).filter(
        PRExtra.customer_name == extra.customer_name,
        PRExtra.customer_ref_no == extra.customer_ref_no,
    ).first()
    if existing_extra:
        raise HTTPException(status_code=400, detail="Combination of customer_name and customer_ref_no already exists")

    db.add(extra)

    # Items
    for it in data.items:
        # Validate item exists
        if not db.get(Item, it.item_id):
            raise HTTPException(status_code=404, detail=f"Item {it.item_id} not found")
        # Backward compatible storage
        qty_kgs = float(it.qty_kgs or 0)
        qty_bags = float(it.qty_bags or 0)
        unit_price = float(it.unit_price or 0)
        # Keep legacy columns populated (qty & target_price)
        legacy_qty = qty_kgs if qty_kgs > 0 else (qty_bags if qty_bags > 0 else 0)
        pr_item = PurchaseRequisitionItem(
            pr_id=pr.id,
            item_id=it.item_id,
            qty=legacy_qty,
            target_price=unit_price,
            notes=it.notes,
        )
        db.add(pr_item)
        db.flush()
        pr_item_extra = PRItemExtra(
            pr_item_id=pr_item.id,
            qty_bags=qty_bags,
            qty_kgs=qty_kgs,
            unit_price=unit_price,
        )
        db.add(pr_item_extra)

    db.commit()
    db.refresh(pr)
    return {"id": pr.id, "pr_number": pr.pr_number}


@app.get("/purchase-requisitions", tags=["purchase_requisition"])
def list_pr(db: Session = Depends(get_db)):
    prs = db.query(PurchaseRequisition).order_by(PurchaseRequisition.id.desc()).all()
    result = []
    for pr in prs:
        items = []
        for i in pr.items:
            items.append(
                {
                    "item_id": i.item_id,
                    "qty_bags": i.extra.qty_bags if i.extra else None,
                    "qty_kgs": i.extra.qty_kgs if i.extra else i.qty,
                    "unit_price": i.extra.unit_price if i.extra else i.target_price,
                    "notes": i.notes,
                }
            )
        extra = pr.extra
        result.append(
            {
                "id": pr.id,
                "pr_number": pr.pr_number,
                "requested_by": pr.requested_by,
                "department": pr.department,
                "requested_date": pr.requested_date,
                "status": pr.status,
                "notes": pr.notes,
                "customer_name": extra.customer_name if extra else None,
                "customer_ref_no": extra.customer_ref_no if extra else None,
                "supplier_name": extra.supplier_name if extra else None,
                "supplier_address": extra.supplier_address if extra else None,
                "tax_code": extra.tax_code if extra else None,
                "tax_percent": extra.tax_percent if extra else None,
                "approval_status": extra.approval_status if extra else None,
                "approval_level": extra.approval_level if extra else None,
                "items": items,
            }
        )
    return result


@app.post("/purchase-requisitions/{pr_id}/approve", tags=["purchase_requisition"])
def approve_pr(pr_id: int, action: ApproveAction, db: Session = Depends(get_db)):
    pr = db.get(PurchaseRequisition, pr_id)
    if not pr or not pr.extra:
        raise HTTPException(status_code=404, detail="PR not found")
    import json
    extra = pr.extra
    stages = []
    try:
        stages = json.loads(extra.approval_flow) if extra.approval_flow else []
    except Exception:
        stages = []

    if action.decision == "reject":
        extra.approval_status = "REJECTED"
        pr.status = "CANCELLED"
        db.commit()
        return {"status": extra.approval_status}

    # Approve path
    current_level = extra.approval_level or 0
    if current_level >= len(stages):
        # Already at end
        extra.approval_status = "APPROVED"
        pr.status = "APPROVED"
    else:
        current_level += 1
        extra.approval_level = current_level
        if current_level >= len(stages):
            extra.approval_status = "APPROVED"
            pr.status = "APPROVED"
        else:
            extra.approval_status = "PENDING"
    db.commit()
    return {"status": extra.approval_status, "level": extra.approval_level}


# -----------------------------
# Purchase Order
# -----------------------------
@app.post("/purchase-orders", tags=["purchase_order"])
def create_po(data: POCreate, db: Session = Depends(get_db)):
    if db.query(PurchaseOrder).filter(PurchaseOrder.po_number == data.po_number).first():
        raise HTTPException(status_code=400, detail="PO number already exists")

    linked_pr = None
    if data.pr_id:
        linked_pr = db.get(PurchaseRequisition, data.pr_id)
        if not linked_pr:
            raise HTTPException(status_code=404, detail="Linked PR not found")

    supplier_id = data.supplier_id
    # If supplier not provided but PR given, derive from PR supplier_name (create master if needed)
    if not supplier_id and linked_pr and linked_pr.extra and linked_pr.extra.supplier_name:
        sup_name = linked_pr.extra.supplier_name
        supplier = db.query(Supplier).filter(Supplier.name == sup_name).first()
        if not supplier:
            supplier = Supplier(name=sup_name, address=linked_pr.extra.supplier_address or None)
            db.add(supplier)
            db.flush()
        supplier_id = supplier.id

    if not supplier_id:
        raise HTTPException(status_code=400, detail="supplier_id is required (or provide pr_id with supplier on PR)")

    po = PurchaseOrder(
        po_number=data.po_number,
        supplier_id=supplier_id,
        pr_id=data.pr_id,
        order_date=data.order_date or date.today(),
        status=data.status,
        notes=data.notes,
    )
    db.add(po)
    db.flush()

    # Snapshot approval flow for PO
    cfg = db.query(ApprovalConfig).filter(ApprovalConfig.doc_type == "PO").first()
    import json
    stages = []
    if cfg:
        try:
            stages = json.loads(cfg.flow_json)
        except Exception:
            stages = []

    # Snapshot tax for PO (prefer payload, else PR's tax)
    tax_code = data.tax_code or (linked_pr.extra.tax_code if linked_pr and linked_pr.extra else None)
    tax_percent = 0.0
    if tax_code:
        today = date.today()
        row = (
            db.query(TaxRate)
            .filter(TaxRate.code == tax_code, TaxRate.is_active == 1, TaxRate.effective_date <= today)
            .order_by(TaxRate.effective_date.desc(), TaxRate.id.desc())
            .first()
        )
        if row:
            tax_percent = float(row.rate_percent)

    po_extra = POExtra(
        po_id=po.id,
        tax_code=tax_code,
        tax_percent=tax_percent,
        approval_status="PENDING",
        approval_level=0,
        approval_flow=json.dumps(stages) if stages else json.dumps([]),
    )
    db.add(po_extra)

    # Items: if items provided use them; else if PR linked, prefill from PR
    po_items_source = data.items
    if (not po_items_source or len(po_items_source) == 0) and linked_pr:
        # derive from PR: use qty_kgs if available else qty_bags else legacy qty; unit price from PR extra.unit_price
        for pri in linked_pr.items:
            unit_price = pri.extra.unit_price if pri.extra else (pri.target_price or 0)
            qty = pri.extra.qty_kgs if (pri.extra and pri.extra.qty_kgs and pri.extra.qty_kgs > 0) else (
                pri.extra.qty_bags if (pri.extra and pri.extra.qty_bags and pri.extra.qty_bags > 0) else pri.qty
            )
            db.add(PurchaseOrderItem(po_id=po.id, item_id=pri.item_id, qty_ordered=float(qty or 0), unit_price=float(unit_price or 0)))
    else:
        for it in po_items_source:
            if not db.get(Item, it.item_id):
                raise HTTPException(status_code=404, detail=f"Item {it.item_id} not found")
            po_item = PurchaseOrderItem(
                po_id=po.id,
                item_id=it.item_id,
                qty_ordered=it.qty_ordered,
                unit_price=it.unit_price,
            )
            db.add(po_item)

    db.commit()
    db.refresh(po)
    return {"id": po.id, "po_number": po.po_number}


@app.get("/purchase-orders", tags=["purchase_order"])
def list_po(db: Session = Depends(get_db)):
    pos = db.query(PurchaseOrder).order_by(PurchaseOrder.id.desc()).all()
    result = []
    for po in pos:
        items = [
            {
                "item_id": i.item_id,
                "qty_ordered": i.qty_ordered,
                "unit_price": i.unit_price,
            }
            for i in po.items
        ]
        extra = po.extra
        result.append(
            {
                "id": po.id,
                "po_number": po.po_number,
                "supplier_id": po.supplier_id,
                "pr_id": po.pr_id,
                "order_date": po.order_date,
                "status": po.status,
                "notes": po.notes,
                "items": items,
                "approval_status": extra.approval_status if extra else None,
                "approval_level": extra.approval_level if extra else None,
                "tax_code": extra.tax_code if extra else None,
                "tax_percent": extra.tax_percent if extra else None,
            }
        )
    return result


@app.get("/purchase-orders/{po_id}/totals", tags=["purchase_order"])
def po_totals(po_id: int, tax_code: Optional[str] = None, db: Session = Depends(get_db)):
    po = db.get(PurchaseOrder, po_id)
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")
    # Subtotal from items
    subtotal = 0.0
    for it in po.items:
        subtotal += (it.qty_ordered * it.unit_price)
    tax_percent = 0.0
    tax_info = None
    # Prefer provided tax_code; else snapshot stored on PO
    use_code = tax_code or (po.extra.tax_code if po.extra else None)
    if use_code:
        # get current tax by code
        today = date.today()
        row = (
            db.query(TaxRate)
            .filter(TaxRate.code == use_code, TaxRate.is_active == 1, TaxRate.effective_date <= today)
            .order_by(TaxRate.effective_date.desc(), TaxRate.id.desc())
            .first()
        )
        if row:
            tax_percent = float(row.rate_percent)
            tax_info = {"code": row.code, "name": row.name, "effective_date": row.effective_date, "rate_percent": row.rate_percent}
    tax_amount = subtotal * (tax_percent / 100.0)
    grand_total = subtotal + tax_amount
    return {
        "po_id": po.id,
        "po_number": po.po_number,
        "subtotal": round(subtotal, 2),
        "tax_percent": tax_percent,
        "tax_amount": round(tax_amount, 2),
        "grand_total": round(grand_total, 2),
        "tax": tax_info,
    }


@app.post("/purchase-orders/{po_id}/approve", tags=["purchase_order"])
def approve_po(po_id: int, action: ApproveAction, db: Session = Depends(get_db)):
    po = db.get(PurchaseOrder, po_id)
    if not po or not po.extra:
        raise HTTPException(status_code=404, detail="PO not found")
    import json
    extra = po.extra
    stages = []
    try:
        stages = json.loads(extra.approval_flow) if extra.approval_flow else []
    except Exception:
        stages = []

    if action.decision == "reject":
        extra.approval_status = "REJECTED"
        po.status = "CANCELLED"
        db.commit()
        return {"status": extra.approval_status}

    current_level = extra.approval_level or 0
    if current_level >= len(stages):
        extra.approval_status = "APPROVED"
        po.status = "APPROVED"
    else:
        current_level += 1
        extra.approval_level = current_level
        if current_level >= len(stages):
            extra.approval_status = "APPROVED"
            po.status = "APPROVED"
        else:
            extra.approval_status = "PENDING"
    db.commit()
    return {"status": extra.approval_status, "level": extra.approval_level}


# -----------------------------
# GRN (Goods Received)
# -----------------------------
@app.post("/grns", tags=["goods_received"])
def create_grn(data: GRNCreate, db: Session = Depends(get_db)):
    if db.query(GRN).filter(GRN.grn_number == data.grn_number).first():
        raise HTTPException(status_code=400, detail="GRN number already exists")
    po = db.get(PurchaseOrder, data.po_id)
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")

    grn = GRN(
        grn_number=data.grn_number,
        po_id=data.po_id,
        received_date=data.received_date or date.today(),
        status=data.status,
        notes=data.notes,
    )
    db.add(grn)
    db.flush()

    # Create GRN items and stock transactions
    for it in data.items:
        if not db.get(Item, it.item_id):
            raise HTTPException(status_code=404, detail=f"Item {it.item_id} not found")
        grn_item = GRNItem(grn_id=grn.id, item_id=it.item_id, qty_received=it.qty_received)
        db.add(grn_item)
        # Stock in
        txn = StockTransaction(
            item_id=it.item_id,
            txn_type="PO_RECEIPT",
            ref_type="GRN",
            ref_id=grn.id,
            qty_delta=it.qty_received,
            notes=f"Receipt via GRN {grn.grn_number} for PO {po.po_number}",
        )
        db.add(txn)

    db.commit()
    db.refresh(grn)
    return {"id": grn.id, "grn_number": grn.grn_number}


@app.get("/grns", tags=["goods_received"])
def list_grn(db: Session = Depends(get_db)):
    grns = db.query(GRN).order_by(GRN.id.desc()).all()
    result = []
    for g in grns:
        items = [
            {"item_id": i.item_id, "qty_received": i.qty_received}
            for i in g.items
        ]
        result.append(
            {
                "id": g.id,
                "grn_number": g.grn_number,
                "po_id": g.po_id,
                "received_date": g.received_date,
                "status": g.status,
                "notes": g.notes,
                "items": items,
            }
        )
    return result


# -----------------------------
# Material Request & Issue (reduces stock)
# -----------------------------
@app.post("/material-requests", tags=["material_request"])
def create_mr(data: MRCreate, db: Session = Depends(get_db)):
    if db.query(MaterialRequest).filter(MaterialRequest.mr_number == data.mr_number).first():
        raise HTTPException(status_code=400, detail="MR number already exists")

    mr = MaterialRequest(
        mr_number=data.mr_number,
        department=data.department,
        requested_by=data.requested_by,
        requested_date=data.requested_date or date.today(),
        status=data.status,
        notes=data.notes,
    )
    db.add(mr)
    db.flush()

    for it in data.items:
        if not db.get(Item, it.item_id):
            raise HTTPException(status_code=404, detail=f"Item {it.item_id} not found")
        mr_item = MaterialRequestItem(
            mr_id=mr.id,
            item_id=it.item_id,
            qty_requested=it.qty_requested,
            qty_issued=it.qty_issued or 0,
        )
        db.add(mr_item)
        if it.qty_issued and it.qty_issued > 0:
            txn = StockTransaction(
                item_id=it.item_id,
                txn_type="ISSUE",
                ref_type="MR",
                ref_id=mr.id,
                qty_delta=-abs(it.qty_issued),
                notes=f"Issue via MR {mr.mr_number}",
            )
            db.add(txn)

    db.commit()
    db.refresh(mr)
    return {"id": mr.id, "mr_number": mr.mr_number}


@app.get("/material-requests", tags=["material_request"])
def list_mr(db: Session = Depends(get_db)):
    mrs = db.query(MaterialRequest).order_by(MaterialRequest.id.desc()).all()
    result = []
    for m in mrs:
        items = [
            {
                "item_id": i.item_id,
                "qty_requested": i.qty_requested,
                "qty_issued": i.qty_issued,
            }
            for i in m.items
        ]
        result.append(
            {
                "id": m.id,
                "mr_number": m.mr_number,
                "department": m.department,
                "requested_by": m.requested_by,
                "requested_date": m.requested_date,
                "status": m.status,
                "notes": m.notes,
                "items": items,
            }
        )
    return result


# -----------------------------
# Reports
# -----------------------------
@app.get("/reports/pr-vs-po", tags=["reports"])
def report_pr_vs_po(pr_id: int = Query(..., description="Purchase Requisition ID"), db: Session = Depends(get_db)):
    pr = db.get(PurchaseRequisition, pr_id)
    if not pr:
        raise HTTPException(status_code=404, detail="PR not found")

    # Sum ordered quantities from all POs linked to this PR
    item_map: Dict[int, Dict[str, float]] = {}
    for it in pr.items:
        item_map[it.item_id] = {"requested": it.qty, "ordered": 0.0}

    pos = db.query(PurchaseOrder).filter(PurchaseOrder.pr_id == pr_id).all()
    for po in pos:
        for it in po.items:
            if it.item_id in item_map:
                item_map[it.item_id]["ordered"] += it.qty_ordered
            else:
                item_map[it.item_id] = {"requested": 0.0, "ordered": it.qty_ordered}

    rows = []
    for item_id, vals in item_map.items():
        item = db.get(Item, item_id)
        rows.append(
            {
                "item_id": item_id,
                "item_name": item.name if item else None,
                "requested_qty": vals["requested"],
                "ordered_qty": vals["ordered"],
                "variance": (vals["ordered"] - vals["requested"]),
            }
        )
    # Carry forward business keys
    extra = pr.extra
    return {
        "pr_id": pr.id,
        "pr_number": pr.pr_number,
        "customer_name": extra.customer_name if extra else None,
        "customer_ref_no": extra.customer_ref_no if extra else None,
        "rows": rows,
    }


@app.get("/reports/po-vs-grn", tags=["reports"])
def report_po_vs_grn(po_id: int = Query(..., description="Purchase Order ID"), db: Session = Depends(get_db)):
    po = db.get(PurchaseOrder, po_id)
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")

    item_map: Dict[int, Dict[str, float]] = {}
    for it in po.items:
        item_map[it.item_id] = {"ordered": it.qty_ordered, "received": 0.0}

    grns = db.query(GRN).filter(GRN.po_id == po_id).all()
    for grn in grns:
        for it in grn.items:
            if it.item_id in item_map:
                item_map[it.item_id]["received"] += it.qty_received
            else:
                item_map[it.item_id] = {"ordered": 0.0, "received": it.qty_received}

    rows = []
    for item_id, vals in item_map.items():
        item = db.get(Item, item_id)
        rows.append(
            {
                "item_id": item_id,
                "item_name": item.name if item else None,
                "ordered_qty": vals["ordered"],
                "received_qty": vals["received"],
                "variance": (vals["received"] - vals["ordered"]),
            }
        )
    return {"po_id": po.id, "po_number": po.po_number, "rows": rows}


# -----------------------------
# Stock Book / Ledger
# -----------------------------
@app.get("/stock/ledger", tags=["stock"])
def stock_ledger(
    item_id: int,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    db: Session = Depends(get_db),
):
    q = db.query(StockTransaction).filter(StockTransaction.item_id == item_id)
    if start:
        q = q.filter(StockTransaction.txn_date >= start)
    if end:
        q = q.filter(StockTransaction.txn_date <= end)
    txns = q.order_by(StockTransaction.txn_date.asc(), StockTransaction.id.asc()).all()

    # Compute running balance
    balance = 0.0
    rows = []
    for t in txns:
        balance += t.qty_delta
        rows.append(
            {
                "id": t.id,
                "txn_date": t.txn_date,
                "txn_type": t.txn_type,
                "ref_type": t.ref_type,
                "ref_id": t.ref_id,
                "qty_delta": t.qty_delta,
                "balance": balance,
                "notes": t.notes,
            }
        )
    return {"item_id": item_id, "transactions": rows}


# Utility endpoint to compute current stock of an item
@app.get("/stock/current", tags=["stock"])
def current_stock(item_id: int, db: Session = Depends(get_db)):
    total = (
        db.query(func.coalesce(func.sum(StockTransaction.qty_delta), 0))
        .filter(StockTransaction.item_id == item_id)
        .scalar()
    )
    return {"item_id": item_id, "current_stock": float(total or 0)}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
