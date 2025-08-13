public interface IEncryption
{
    // per-ramka WAL (AEAD)
    ReadOnlyMemory<byte> Encrypt(ReadOnlySpan<byte> plaintext, Span<byte> nonce, out ReadOnlyMemory<byte> tag);
    bool TryDecrypt(ReadOnlySpan<byte> ciphertext, ReadOnlySpan<byte> nonce, ReadOnlySpan<byte> tag, out byte[] plaintext);

    // identyfikacja algorytmu/wersji
    byte AlgoVersion { get; }
}
