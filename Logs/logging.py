from datetime import datetime
import glob
import os

def cleanup_old_logs(max_logs=8):
    logs = sorted(
        glob.glob("insert_log_*.txt"),
        key=os.path.getmtime,
        reverse=True
    )

    for old_log in logs[max_logs:]:
        if os.path.exists(old_log):  
            os.remove(old_log)

    

def create_log():
    log_filename = f"insert_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_file = open(log_filename, "w", encoding="utf-8")
    return log_file