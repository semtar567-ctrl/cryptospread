import sqlite3

def migrate():
    db_path = "cryptospread.db"
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Проверяем наличие колонки
        cursor.execute("PRAGMA table_info(tracked_spreads)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "tracking_mode" not in columns:
            print("Adding column 'tracking_mode' to 'tracked_spreads'...")
            cursor.execute("ALTER TABLE tracked_spreads ADD COLUMN tracking_mode TEXT DEFAULT 'net'")
            print("✅ Column added successfully.")
        else:
            print("Column 'tracking_mode' already exists.")

        conn.commit()
    except Exception as e:
        print(f"❌ Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
