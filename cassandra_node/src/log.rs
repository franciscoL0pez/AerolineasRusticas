use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::DateTime;

/// This struct represents the logger for each node.
/// 
/// 
#[derive(Clone, Debug)]
pub struct Logger {
    id: String, // Identificador del nodo o logger
}

impl Logger {
    pub fn new(id: &str) -> Self {
        Logger { id: id.to_string() }
    }

    /// Creates log and writes the message recieved.
    /// 
    /// #Parameters
    /// - `message`: String with the message to log.
    /// 
    pub fn log(&self, message: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Crear directorio de logs si no existe
        let log_dir = "logs";
        create_dir_all(log_dir)?;

        // Ruta del archivo de log
        let log_path = format!("{}/{}.log", log_dir, self.id);
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(log_path)?;

        // Obtener el tiempo actual
        let time = SystemTime::now();
        let duration = time.duration_since(UNIX_EPOCH)?;

        // Convertir los segundos y nanosegundos en una fecha legible
        let secs = duration.as_secs();
        let nanos = duration.subsec_nanos();
        let naive_date = DateTime::from_timestamp(secs as i64, nanos).expect("Timestamp inválido");
        let timestamp = naive_date.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

        // Formatear el mensaje y escribir en el archivo
        let to_log = format!("[{}] {}: {}\n", timestamp, self.id, message);
        file.write_all(to_log.as_bytes())?;

        // Imprimir también en stdout
        println!("{}", to_log);
        Ok(())
    }
}
