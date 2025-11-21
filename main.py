import os
from datetime import datetime, date
from typing import List, Optional

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
    Enum,
    Text,
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
    qty: float
    target_price: float = 0
    notes: Optional[str] = None


class PRCreate(BaseModel):
    pr_number: str
    requested_by: Optional[str] = None
    department: Optional[str] = None
    requested_date: Optional[date] = None
    status: str = "OPEN"
    notes: Optional[str] = None
    items: List[PRItemIn]


class PROut(BaseModel):
    id: int
    pr_number: str
    requested_by: Optional[str]
    department: Optional[str]
    requested_date: date
    status: str
    notes: Optional[str]
    items: List[PRItemIn]

    class Config:
        from_attributes = True


class POItemIn(BaseModel):
    item_id: int
    qty_ordered: float
    unit_price: float


class POCreate(BaseModel):
    po_number: str
    supplier_id: int
    pr_id: Optional[int] = None
    order_date: Optional[date] = None
    status: str = "OPEN"
    notes: Optional[str] = None
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
# Purchase Requisition
# -----------------------------
@app.post("/purchase-requisitions", tags=["purchase_requisition"])
def create_pr(data: PRCreate, db: Session = Depends(get_db)):
    if db.query(PurchaseRequisition).filter(PurchaseRequisition.pr_number == data.pr_number).first():
        raise HTTPException(status_code=400, detail="PR number already exists")
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

    for it in data.items:
        # Validate item exists
        if not db.get(Item, it.item_id):
            raise HTTPException(status_code=404, detail=f"Item {it.item_id} not found")
        pr_item = PurchaseRequisitionItem(
            pr_id=pr.id,
            item_id=it.item_id,
            qty=it.qty,
            target_price=it.target_price,
            notes=it.notes,
        )
        db.add(pr_item)

    db.commit()
    db.refresh(pr)
    return {"id": pr.id, "pr_number": pr.pr_number}


@app.get("/purchase-requisitions", tags=["purchase_requisition"])
def list_pr(db: Session = Depends(get_db)):
    prs = db.query(PurchaseRequisition).order_by(PurchaseRequisition.id.desc()).all()
    result = []
    for pr in prs:
        items = [
            {
                "item_id": i.item_id,
                "qty": i.qty,
                "target_price": i.target_price,
                "notes": i.notes,
            }
            for i in pr.items
        ]
        result.append(
            {
                "id": pr.id,
                "pr_number": pr.pr_number,
                "requested_by": pr.requested_by,
                "department": pr.department,
                "requested_date": pr.requested_date,
                "status": pr.status,
                "notes": pr.notes,
                "items": items,
            }
        )
    return result


# -----------------------------
# Purchase Order
# -----------------------------
@app.post("/purchase-orders", tags=["purchase_order"])
def create_po(data: POCreate, db: Session = Depends(get_db)):
    if db.query(PurchaseOrder).filter(PurchaseOrder.po_number == data.po_number).first():
        raise HTTPException(status_code=400, detail="PO number already exists")
    if not db.get(Supplier, data.supplier_id):
        raise HTTPException(status_code=404, detail="Supplier not found")
    if data.pr_id and not db.get(PurchaseRequisition, data.pr_id):
        raise HTTPException(status_code=404, detail="Linked PR not found")

    po = PurchaseOrder(
        po_number=data.po_number,
        supplier_id=data.supplier_id,
        pr_id=data.pr_id,
        order_date=data.order_date or date.today(),
        status=data.status,
        notes=data.notes,
    )
    db.add(po)
    db.flush()

    for it in data.items:
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
            }
        )
    return result


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
    item_map = {}
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
    return {"pr_id": pr.id, "pr_number": pr.pr_number, "rows": rows}


@app.get("/reports/po-vs-grn", tags=["reports"])
def report_po_vs_grn(po_id: int = Query(..., description="Purchase Order ID"), db: Session = Depends(get_db)):
    po = db.get(PurchaseOrder, po_id)
    if not po:
        raise HTTPException(status_code=404, detail="PO not found")

    item_map = {}
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
