# Tugas 2 Big Data

Repositori ini berisi kode untuk Tugas 2 Big Data yang berfokus pada transformasi dan pengolahan data saham dan berita pasar.

## Struktur Proyek

```
Tugas-2-Big-Data/
├── Tugas 2/
│   ├── IDX Financial Report/
│   │   └── idx_transform.py
│   ├── IQPLUS News/
│   │   ├── iqplus_market_transform.py
│   │   └── iqplus_stock_transform.py
│   └── Yfinance API/
│       ├── Daftar_Saham.csv
│       └── yfinance_transform.py
```

## IDX Financial Report

### Deskripsi
Program ini berfungsi untuk mengambil data saham dari MongoDB, melakukan transformasi dan agregasi menggunakan PySpark, kemudian menyimpan hasil transformasi kembali ke MongoDB.

### Fitur
- Membaca daftar nama perusahaan dari CSV
- Transformasi data saham menggunakan PySpark
- Agregasi data berdasarkan periode:
  - Harian (yyyy-MM-dd)
  - Bulanan (yyyy-MM)
  - Tahunan (yyyy)
- Penghitungan metrik keuangan komprehensif:
  - Rata-rata (avg)
  - Total (sum)
  - Nilai tertinggi (max)
  - Nilai terendah (min)
  - Standar deviasi (std)

### Konfigurasi
- **Database Sumber**: `Yfinance`
- **Database Tujuan**: `Yfinance_Final`
- **File Daftar Saham**: `Daftar_Saham.csv`

### Cara Penggunaan
```bash
python idx_transform.py
```

## IQPLUS News

### Deskripsi
Modul ini terdiri dari dua script untuk mengolah dan meringkas berita pasar saham menggunakan model AI. Script mengambil data berita dari MongoDB, melakukan ringkasan konten berita, dan menyimpan hasilnya kembali ke MongoDB.

### Script 1: iqplus_market_transform.py
Memproses berita pasar umum dari koleksi "MarketNews" dan menyimpan ringkasannya ke "RingkasanMarketNews".

### Script 2: iqplus_stock_transform.py
Memproses berita saham spesifik dari koleksi "StockNews" dan menyimpan ringkasannya ke "RingkasanStockNews".

### Fitur
- Peringkasan konten berita dengan model AI BART
- Pemrosesan teks panjang menggunakan algoritma split-merge
- Pemberian indeks pada setiap dokumen berita
- Pencatatan log aktivitas pemrosesan

### Konfigurasi
- **Database**: `BeritaPasar`
- **Model AI**: `facebook/bart-large-cnn`
- **Parameter Ringkasan**:
  - Panjang maksimum: 250 token
  - Panjang minimum: 50 token

### Cara Penggunaan
```bash
# Untuk ringkasan berita pasar
python iqplus_market_transform.py

# Untuk ringkasan berita saham
python iqplus_stock_transform.py
```

## Yfinance API

### Deskripsi
Modul ini mengambil data historis saham dari Yahoo Finance API, melakukan transformasi dengan PySpark, dan menyimpan hasil agregasi ke dalam MongoDB.

### Fitur
- Pengambilan data dari daftar saham yang disediakan dalam format CSV
- Transformasi data dengan PySpark
- Agregasi data berdasarkan periode waktu yang berbeda:
  - Harian (day)
  - Bulanan (month)
  - Tahunan (year)
- Penghitungan berbagai metrik untuk analisis:
  - Rata-rata (avg)
  - Jumlah (sum)
  - Nilai tertinggi (max)
  - Nilai terendah (min)
  - Standar deviasi (std)

### Konfigurasi
- **Database Sumber**: `Yfinance`
- **Database Tujuan**: `Yfinance_Final`
- **File Daftar Saham**: `Daftar_Saham.csv` berisi kode dan nama perusahaan

### Format Data Saham
File `Daftar_Saham.csv` mengandung daftar saham dengan format:
```
Kode,Nama Perusahaan
AALI,Astra Agro Lestari Tbk
ABBA,Mahaka Media Tbk
...
```

### Cara Penggunaan
```bash
python yfinance_transform.py
```

## Prasyarat
- Python 3.x
- PySpark
- PyMongo
- MongoDB (terinstal dan berjalan di localhost:27017)
- Transformers (Hugging Face)
- Yahoo Finance API

## Instalasi Dependensi
```bash
pip install pymongo pyspark transformers torch yfinance
```

## Pengembangan Lebih Lanjut
- Penambahan fitur visualisasi data
- Implementasi analisis sentimen berita
- Integrasi dengan dashboard monitoring
- Analisis korelasi antara berita dan pergerakan harga saham

---
