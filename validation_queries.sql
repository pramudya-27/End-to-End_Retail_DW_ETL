-- =====================================================================
-- VALIDATION & RECONCILIATION QUERIES: ONLINE RETAIL DATA WAREHOUSE
-- Database: online_retail_dw
-- Enterprise Data Warehouse & Business Intelligence Project
-- =====================================================================

USE `online_retail_dw`;

-- ---------------------------------------------------------------------
-- 1. CEK STRUKTUR TABEL & RELASI (DESCRIBE TABEL)
-- ---------------------------------------------------------------------
DESCRIBE `dim_date`;
DESCRIBE `dim_product`;
DESCRIBE `dim_customer`;
DESCRIBE `dim_country`;
DESCRIBE `fact_sales`;

-- ---------------------------------------------------------------------
-- 2. JUMLAH DATA MASING-MASING TABEL DIMENSI & FAKTA
-- ---------------------------------------------------------------------
SELECT 'dim_date' AS Tabel, COUNT(*) AS Total_Baris FROM `dim_date`
UNION ALL
SELECT 'dim_product' AS Tabel, COUNT(*) AS Total_Baris FROM `dim_product`
UNION ALL
SELECT 'dim_customer' AS Tabel, COUNT(*) AS Total_Baris FROM `dim_customer`
UNION ALL
SELECT 'dim_country' AS Tabel, COUNT(*) AS Total_Baris FROM `dim_country`
UNION ALL
SELECT 'fact_sales' AS Tabel, COUNT(*) AS Total_Baris FROM `fact_sales`;

-- ---------------------------------------------------------------------
-- 3. REKONSILIASI METRIK FAKTA (TOTAL QUANTITY, GROSS SALES, INVOICE)
-- ---------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_transaksi_fakta,
    SUM(Quantity) AS total_quantity,
    SUM(GrossSales) AS total_gross_sales,
    COUNT(DISTINCT InvoiceNo) AS total_invoice_unik,
    COUNT(DISTINCT ProductKey) AS total_produk_terjual,
    COUNT(DISTINCT CustomerKey) AS total_pelanggan_transaksi
FROM `fact_sales`;

-- ---------------------------------------------------------------------
-- 4. CEK ANGGOTA DIMENSI KHUSUS 'UNKNOWN' PADA DIM_CUSTOMER
-- ---------------------------------------------------------------------
-- Pelanggan 'Unknown' dibuat untuk menampung transaksi tanpa CustomerID
SELECT 
    CustomerKey, 
    COALESCE(CAST(CustomerID AS CHAR), 'Unknown (NULL Asli)') AS Status_CustomerID,
    COUNT(*) AS Total_Baris
FROM `dim_customer`
WHERE CustomerID IS NULL;

-- Total transaksi yang dialokasikan ke pelanggan Unknown
SELECT 
    dc.CustomerKey,
    'Unknown Customer' AS Keterangan,
    COUNT(fs.SalesKey) AS Total_Transaksi,
    SUM(fs.Quantity) AS Total_Qty,
    SUM(fs.GrossSales) AS Total_GrossSales
FROM `fact_sales` fs
JOIN `dim_customer` dc ON fs.CustomerKey = dc.CustomerKey
WHERE dc.CustomerID IS NULL
GROUP BY dc.CustomerKey;

-- ---------------------------------------------------------------------
-- 5. CEK INTEGRITAS REFERENSIAL (ORPHAN FOREIGN KEY CHECK)
-- Hasil semua kolom HARUS 0 jika referential integrity terjaga 100%
-- ---------------------------------------------------------------------
SELECT
    (SELECT COUNT(*) 
     FROM `fact_sales` fs 
     LEFT JOIN `dim_date` dd ON fs.DateKey = dd.DateKey 
     WHERE dd.DateKey IS NULL) AS Orphan_Date_FK,

    (SELECT COUNT(*) 
     FROM `fact_sales` fs 
     LEFT JOIN `dim_product` dp ON fs.ProductKey = dp.ProductKey 
     WHERE dp.ProductKey IS NULL) AS Orphan_Product_FK,

    (SELECT COUNT(*) 
     FROM `fact_sales` fs 
     LEFT JOIN `dim_customer` dc ON fs.CustomerKey = dc.CustomerKey 
     WHERE dc.CustomerKey IS NULL) AS Orphan_Customer_FK,

    (SELECT COUNT(*) 
     FROM `fact_sales` fs 
     LEFT JOIN `dim_country` dcn ON fs.CountryKey = dcn.CountryKey 
     WHERE dcn.CountryKey IS NULL) AS Orphan_Country_FK;

-- ---------------------------------------------------------------------
-- 6. SAMPEL JOIN FACT_SALES DENGAN 4 TABEL DIMENSI LENGKAP
-- Membuktikan relasi Star Schema berfungsi sempurna
-- ---------------------------------------------------------------------
SELECT
    fs.SalesKey,
    fs.InvoiceNo,
    dd.FullDate,
    dd.MonthName,
    dd.Year,
    dp.StockCode,
    dp.Description AS Nama_Produk,
    COALESCE(CAST(dc.CustomerID AS CHAR), 'Unknown') AS CustomerID,
    dcn.CountryName AS Negara,
    fs.Quantity,
    fs.UnitPrice,
    fs.GrossSales
FROM `fact_sales` fs
JOIN `dim_date` dd ON fs.DateKey = dd.DateKey
JOIN `dim_product` dp ON fs.ProductKey = dp.ProductKey
JOIN `dim_customer` dc ON fs.CustomerKey = dc.CustomerKey
JOIN `dim_country` dcn ON fs.CountryKey = dcn.CountryKey
ORDER BY fs.SalesKey ASC
LIMIT 15;

-- ---------------------------------------------------------------------
-- 7. QUERY ANALITIK KECERDASAN BISNIS (BI SAMPLE QUERIES)
-- ---------------------------------------------------------------------

-- A. Top 10 Produk Berdasarkan Total Gross Sales
SELECT 
    dp.StockCode,
    dp.Description,
    SUM(fs.Quantity) AS Total_Kuantitas,
    SUM(fs.GrossSales) AS Total_Pendapatan
FROM `fact_sales` fs
JOIN `dim_product` dp ON fs.ProductKey = dp.ProductKey
GROUP BY dp.ProductKey, dp.StockCode, dp.Description
ORDER BY Total_Pendapatan DESC
LIMIT 10;

-- B. Total Penjualan per Negara
SELECT 
    dcn.CountryName,
    COUNT(DISTINCT fs.InvoiceNo) AS Jumlah_Invoice,
    SUM(fs.Quantity) AS Total_Kuantitas,
    SUM(fs.GrossSales) AS Total_Pendapatan
FROM `fact_sales` fs
JOIN `dim_country` dcn ON fs.CountryKey = dcn.CountryKey
GROUP BY dcn.CountryKey, dcn.CountryName
ORDER BY Total_Pendapatan DESC
LIMIT 10;

-- C. Tren Penjualan Bulanan
SELECT 
    dd.Year,
    dd.Month,
    dd.MonthName,
    COUNT(DISTINCT fs.InvoiceNo) AS Jumlah_Invoice,
    SUM(fs.Quantity) AS Total_Kuantitas,
    SUM(fs.GrossSales) AS Total_Pendapatan
FROM `fact_sales` fs
JOIN `dim_date` dd ON fs.DateKey = dd.DateKey
GROUP BY dd.Year, dd.Month, dd.MonthName
ORDER BY dd.Year ASC, dd.Month ASC;
