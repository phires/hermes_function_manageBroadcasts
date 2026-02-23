void throwIfMissing(Map<String, String> obj, List<String> keys) {
  final missing = keys
      .where((key) => obj[key] == null || obj[key]!.isEmpty)
      .toList();
  if (missing.isNotEmpty) {
    throw Exception('Missing required fields: ${missing.join(', ')}');
  }
}
