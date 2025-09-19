redis_db = {
    "podhub-keys": 0,
    "podhub-data": 1,
    "pressure_bed": 2,
    "insole": 3,
    "undefined": 15,
}

skiin = {
    "name": "skiin",
    "redis_db_keys": redis_db["podhub-keys"],
    "redis_db": redis_db["podhub-data"],
    "char": ["ecg", "acc", "gyro", "cbt", "activity"],
}

pressure_bed = {
    "name": "pressure_bed",
    "redis_db": redis_db["pressure_bed"],
    "char": ["press"],
}

insole = {
    "name": "insole",
    "redis_db": redis_db["insole"],
    "char": ["press", "imu", "imu_sflp"],
}

undefined = {"name": "undefined", "redis_db": redis_db["undefined"], "char": "unknown"}

devices = {
    "skiin": skiin,
    "pressure_bed": pressure_bed,
    "insole": insole,
    "undefined": undefined,
}
