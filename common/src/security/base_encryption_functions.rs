pub fn encrypt(data: &[u8], key: u64) -> Vec<u8> {
    let shift_amount = (key % 8) as u8;
    data.iter()
        .map(|&byte| {
            // XOR with the key (using the lower byte of the key)
            let mut encrypted_byte = byte ^ (key as u8);

            // Apply bitwise NOT (negation)
            encrypted_byte = !encrypted_byte;

            // Bitwise shifts for extra obfuscation
            encrypted_byte = encrypted_byte.rotate_left(shift_amount as u32);
            encrypted_byte = encrypted_byte.rotate_right(shift_amount as u32 / 2);

            encrypted_byte
        })
        .collect()
}

pub fn decrypt(data: &[u8], key: u64) -> Vec<u8> {
    let shift_amount = (key % 8) as u8;
    data.iter()
        .map(|&byte| {
            // Reverse the bitwise shifts in opposite order
            let mut decrypted_byte = byte.rotate_left(shift_amount as u32 / 2);
            decrypted_byte = decrypted_byte.rotate_right(shift_amount as u32);

            // Reverse the bitwise NOT (negation)
            decrypted_byte = !decrypted_byte;

            // Reverse the XOR with the key
            decrypted_byte ^= key as u8;

            decrypted_byte
        })
        .collect()
}
