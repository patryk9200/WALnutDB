public interface IEncryption
{
    // Zwraca świeżą tablicę bajtów (ciphertext). Implementacja powinna być AEAD.
    byte[] Encrypt(ReadOnlySpan<byte> plaintext, string table, ReadOnlySpan<byte> primaryKey);

    // Zwraca plaintext. Rzuca przy błędnym tagu/wersji/formatcie.
    byte[] Decrypt(ReadOnlySpan<byte> ciphertext, string table, ReadOnlySpan<byte> primaryKey);
}