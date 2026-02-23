import 'dart:io';
import 'dart:async';

import 'package:dart_appwrite/dart_appwrite.dart' as appwrite;
import 'utils.dart';

/// Appwrite Function for managing broadcast messages.
///
/// Handles create / update / delete with proper document-level permissions
/// so that all targeted users can read the broadcast.
/// On create, it also sends a push notification to all targeted users.
///
/// Expected request body:
/// {
///   "action": "create" | "update" | "delete",
///
///   // --- create ---
///   "text":        "<string>",                       (required)
///   "priority":    "low" | "normal" | "high" | "urgent",  (required)
///   "videoUrl":    "<string>",                       (optional)
///   "isActive":    <bool>,                           (optional, default true)
///   "targetType":  "all" | "labels" | "users",       (required)
///   "targetLabels": ["admin","security",...],         (required if targetType == "labels")
///   "targetUserIds": ["userId1","userId2",...],       (required if targetType == "users")
///
///   // --- update ---
///   "documentId":  "<document_$id>",                 (required)
///   "text":        "<string>",                       (optional)
///   "priority":    "<string>",                       (optional)
///   "videoUrl":    "<string>",                       (optional)
///   "isActive":    <bool>,                           (optional)
///
///   // --- delete ---
///   "documentId":  "<document_$id>"                  (required)
/// }
Future<dynamic> main(final context) async {
  throwIfMissing(Platform.environment, [
    'APPWRITE_DATABASE_ID',
    'APPWRITE_BROADCAST_MESSAGES_COLLECTION_ID',
    'APPWRITE_USERS_COLLECTION_ID',
  ]);

  if (context.req.method != 'POST') {
    context.log('Invalid request method: ${context.req.method}');
    return context.res.json({'ok': false, 'error': 'invalid request'}, 405);
  }

  try {
    // ── Build Appwrite client (server-side, API key) ──────────────────
    final endpoint = Platform.environment['APPWRITE_ENDPOINT'] ?? '';
    final projectId =
        Platform.environment['APPWRITE_FUNCTION_PROJECT_ID'] ?? '';
    final client = appwrite.Client()
        .setEndpoint(endpoint)
        .setProject(projectId)
        .setKey(context.req.headers['x-appwrite-key']);

    final database = appwrite.TablesDB(client);
    final messaging = appwrite.Messaging(client);
    final storage = appwrite.Storage(client);

    final String databaseId = Platform.environment['APPWRITE_DATABASE_ID']!;
    final String broadcastCollectionId =
        Platform.environment['APPWRITE_BROADCAST_MESSAGES_COLLECTION_ID']!;
    final String usersCollectionId =
        Platform.environment['APPWRITE_USERS_COLLECTION_ID']!;

    context.log('Client created ($endpoint | $projectId)');

    // ── Parse action ──────────────────────────────────────────────────
    final body = context.req.bodyJson as Map<String, dynamic>;
    final action = (body['action'] as String?)?.trim().toLowerCase() ?? '';

    if (action.isEmpty) {
      return context.res.json({
        'ok': false,
        'error': '"action" is required',
      }, 400);
    }

    context.log('Action: $action');

    switch (action) {
      case 'create':
        return await _handleCreate(
          context,
          database,
          storage,
          messaging,
          databaseId,
          broadcastCollectionId,
          usersCollectionId,
          body,
        );
      case 'update':
        return await _handleUpdate(
          context,
          database,
          databaseId,
          broadcastCollectionId,
          body,
        );
      case 'delete':
        return await _handleDelete(
          context,
          database,
          databaseId,
          broadcastCollectionId,
          body,
        );
      default:
        return context.res.json({
          'ok': false,
          'error': 'Unknown action "$action"',
        }, 400);
    }
  } catch (e) {
    context.log('Error: $e');
    return context.res.json({'ok': false, 'error': 'Error: $e'}, 500);
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═════════════════════════════════════════════════════════════════════════════

/// Fetch ALL user documents from the users collection (paginated).
Future<List<dynamic>> _fetchAllUsers(
  appwrite.TablesDB database,
  String databaseId,
  String usersCollectionId,
) async {
  final allUsers = <dynamic>[];
  int offset = 0;
  const batchSize = 100;
  bool hasMore = true;

  while (hasMore) {
    final response = await database.listRows(
      databaseId: databaseId,
      tableId: usersCollectionId,
      queries: [appwrite.Query.limit(batchSize), appwrite.Query.offset(offset)],
    );
    allUsers.addAll(response.rows);
    offset += batchSize;
    hasMore = response.rows.length == batchSize;
  }

  return allUsers;
}

/// Resolve target user Account-IDs based on targetType.
/// Returns a list of Appwrite Account-IDs (used for permissions and push).
///
/// User documents have an `id` field that contains the Account-ID.
Future<List<String>> _resolveTargetAccountIds(
  dynamic context,
  appwrite.TablesDB database,
  String databaseId,
  String usersCollectionId,
  Map<String, dynamic> body,
) async {
  final targetType =
      (body['targetType'] as String?)?.trim().toLowerCase() ?? 'all';

  if (targetType == 'users') {
    // Specific user IDs provided — these are Collection-DocIDs (== Account IDs in this setup)
    final targetUserIds = (body['targetUserIds'] as List?)
            ?.map((e) => e.toString().trim())
            .where((e) => e.isNotEmpty)
            .toList() ??
        [];
    if (targetUserIds.isEmpty) {
      throw Exception(
        'targetUserIds must not be empty when targetType is "users"',
      );
    }
    context.log('Target: ${targetUserIds.length} specific users');
    return targetUserIds;
  }

  // For "all" and "labels" we need to fetch all users
  final allUsers = await _fetchAllUsers(
    database,
    databaseId,
    usersCollectionId,
  );
  context.log('Fetched ${allUsers.length} total users');

  if (targetType == 'labels') {
    final targetLabels = (body['targetLabels'] as List?)
            ?.map((e) => e.toString().trim().toLowerCase())
            .where((e) => e.isNotEmpty)
            .toSet() ??
        {};
    if (targetLabels.isEmpty) {
      throw Exception(
        'targetLabels must not be empty when targetType is "labels"',
      );
    }

    context.log('Filtering by labels: ${targetLabels.join(', ')}');

    final matchingIds = <String>[];
    for (final user in allUsers) {
      final userLabels = (user.data['labels'] as List?)
              ?.map((e) => e.toString().toLowerCase())
              .toSet() ??
          {};
      if (userLabels.intersection(targetLabels).isNotEmpty) {
        // The Account-ID is in the `id` field of the user document
        final accountId = user.data['id'] as String?;
        if (accountId != null && accountId.isNotEmpty) {
          matchingIds.add(accountId);
        }
      }
    }

    context.log('Found ${matchingIds.length} users matching labels');
    return matchingIds;
  }

  // targetType == "all" (default)
  context.log('Target: ALL users');
  final allIds = <String>[];
  for (final user in allUsers) {
    final accountId = user.data['id'] as String?;
    if (accountId != null && accountId.isNotEmpty) {
      allIds.add(accountId);
    }
  }
  return allIds;
}

// ═════════════════════════════════════════════════════════════════════════════
// CREATE
// ═════════════════════════════════════════════════════════════════════════════
// ─────────────────────────────────────────────────────────────────────────────
// NOTE: For video attachment, set these additional env vars in the Appwrite
// Function settings:
//   APPWRITE_MEDIA_FILES_COLLECTION_ID  (e.g. 687d1a25003639ab01fa)
//   APPWRITE_FILE_ACCESS_COLLECTION_ID  (e.g. 687d1adf002fda9ac61f)
//   APPWRITE_MEDIA_FILES_BUCKET_ID      (e.g. 687d19be0028cbdd997d)
// Without them, broadcasts still work – just without file-manager integration.
// ─────────────────────────────────────────────────────────────────────────────
Future<dynamic> _handleCreate(
  dynamic context,
  appwrite.TablesDB database,
  appwrite.Storage storage,
  appwrite.Messaging messaging,
  String databaseId,
  String broadcastCollectionId,
  String usersCollectionId,
  Map<String, dynamic> body,
) async {
  final text = (body['text'] as String?)?.trim() ?? '';
  final priority =
      (body['priority'] as String?)?.trim().toLowerCase() ?? 'normal';
  final videoUrl = (body['videoUrl'] as String?)?.trim();
  final storageFileId = (body['storageFileId'] as String?)?.trim();
  final adminUserId = (body['adminUserId'] as String?)?.trim() ?? '';
  final videoMimeType =
      (body['videoMimeType'] as String?)?.trim() ?? 'video/mp4';
  final videoFileName =
      (body['videoFileName'] as String?)?.trim() ?? 'broadcast_video.mp4';
  final videoFileSizeBytes = body['videoFileSizeBytes'] is int
      ? body['videoFileSizeBytes'] as int
      : int.tryParse(body['videoFileSizeBytes']?.toString() ?? '') ?? 0;
  final isActive = body['isActive'] as bool? ?? true;

  if (text.isEmpty) {
    return context.res.json({
      'ok': false,
      'error': '"text" is required for create',
    }, 400);
  }

  // 1. Resolve target users
  final targetAccountIds = await _resolveTargetAccountIds(
    context,
    database,
    databaseId,
    usersCollectionId,
    body,
  );

  if (targetAccountIds.isEmpty) {
    return context.res.json({
      'ok': false,
      'error': 'No target users found',
    }, 400);
  }

  context.log('Resolved ${targetAccountIds.length} target account IDs');

  // 2. Build document-level permissions: every target user gets read access
  final permissions = <String>[];
  for (final accountId in targetAccountIds) {
    permissions.add('read("user:$accountId")');
  }
  // Admin team gets full CRUD
  permissions.add('read("team:admin")');
  permissions.add('update("team:admin")');
  permissions.add('delete("team:admin")');

  // 3. Build document data
  // Normalize priority to UPPERCASE so the Flutter app can match on "HIGH"/"MEDIUM"
  final normalizedPriority = priority.toUpperCase();

  final docData = <String, dynamic>{
    'id': appwrite.ID.unique(), // The app uses this `id` field, not `$id`
    'text': text,
    'priority': normalizedPriority,
    'created_at': DateTime.now().millisecondsSinceEpoch,
    'is_active': isActive,
  };

  // Prefer uploaded storage file over legacy URL.
  // We encode the Appwrite storage file ID as "storage://<fileId>" inside
  // the existing video_url field, which avoids any schema changes.
  if (storageFileId != null && storageFileId.isNotEmpty) {
    docData['video_url'] = 'storage://$storageFileId';
  } else if (videoUrl != null && videoUrl.isNotEmpty) {
    docData['video_url'] = videoUrl;
  }

  final rowId = appwrite.ID.unique();

  context.log(
    'Creating broadcast (priority: $normalizedPriority, docId: $rowId)',
  );

  final doc = await database.createRow(
    databaseId: databaseId,
    tableId: broadcastCollectionId,
    rowId: rowId,
    data: docData,
    permissions: permissions,
  );

  context.log('Broadcast created: ${doc.$id}');

  // 4. Send push notification to all targeted users
  int pushSent = 0;
  String pushError = '';
  if (isActive) {
    try {
      context.log(
        'Sending push notification to ${targetAccountIds.length} users',
      );
      final pushMessage = await messaging.createPush(
        messageId: appwrite.ID.unique(),
        title: normalizedPriority == 'URGENT' || normalizedPriority == 'HIGH'
            ? '⚠️ Broadcast: $normalizedPriority'
            : '📢 Broadcast',
        body: text.length > 200 ? '${text.substring(0, 200)}...' : text,
        users: targetAccountIds,
        data: {
          'type': 'broadcast',
          'broadcastId': doc.$id,
          'priority': normalizedPriority,
        },
      );
      pushSent = targetAccountIds.length;
      context.log(
        'Push notification sent: ${pushMessage.$id} (status: ${pushMessage.status})',
      );
    } catch (e) {
      pushError = e.toString();
      context.log('Push notification failed: $e');
    }
  }

  // 5. Attach video file to the file manager for all recipients (Option B –
  //    no owner entry; video appears in each recipient's "Shared with me" list)
  int fileAccessCount = 0;
  String fileAttachError = '';
  if (storageFileId != null && storageFileId.isNotEmpty) {
    final mediaFilesCollectionId =
        Platform.environment['APPWRITE_MEDIA_FILES_COLLECTION_ID'] ?? '';
    final fileAccessCollectionId =
        Platform.environment['APPWRITE_FILE_ACCESS_COLLECTION_ID'] ?? '';
    final mediaFilesBucketId =
        Platform.environment['APPWRITE_MEDIA_FILES_BUCKET_ID'] ?? '';

    if (mediaFilesCollectionId.isNotEmpty &&
        fileAccessCollectionId.isNotEmpty &&
        mediaFilesBucketId.isNotEmpty) {
      try {
        fileAccessCount = await _attachVideoFile(
          context: context,
          database: database,
          storage: storage,
          databaseId: databaseId,
          storageFileId: storageFileId,
          broadcastDocId: doc.$id,
          targetAccountIds: targetAccountIds,
          adminUserId: adminUserId,
          mimeType: videoMimeType,
          fileName: videoFileName,
          fileSizeBytes: videoFileSizeBytes,
          mediaFilesCollectionId: mediaFilesCollectionId,
          fileAccessCollectionId: fileAccessCollectionId,
          mediaFilesBucketId: mediaFilesBucketId,
        );
      } catch (e) {
        fileAttachError = e.toString();
        context.log('Warning: video attachment failed: $e');
        // Broadcast was still created — don't fail the whole request
      }
    } else {
      fileAttachError =
          'Media-file env vars not configured — video broadcast created but not shared to file manager';
      context.log('Warning: $fileAttachError');
    }
  }

  return context.res.json({
    'ok': true,
    'result': 'Broadcast created',
    'document': {
      '\$id': doc.$id,
      '\$createdAt': doc.$createdAt,
      '\$updatedAt': doc.$updatedAt,
      ...doc.data,
    },
    'targetCount': targetAccountIds.length,
    'pushSent': pushSent,
    'pushError': pushError,
    'fileAccessCount': fileAccessCount,
    if (fileAttachError.isNotEmpty) 'fileAttachError': fileAttachError,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// ATTACH VIDEO FILE
// ═════════════════════════════════════════════════════════════════════════════
/// Sets Storage + DB permissions for a newly uploaded broadcast video file,
/// creates a mediaFiles record, and creates file_access 'read' entries for
/// every recipient.  No 'owner' entry → appears in "Shared with me".
Future<int> _attachVideoFile({
  required dynamic context,
  required appwrite.TablesDB database,
  required appwrite.Storage storage,
  required String databaseId,
  required String storageFileId,
  required String broadcastDocId,
  required List<String> targetAccountIds,
  required String adminUserId,
  required String mimeType,
  required String fileName,
  required int fileSizeBytes,
  required String mediaFilesCollectionId,
  required String fileAccessCollectionId,
  required String mediaFilesBucketId,
}) async {
  final now = DateTime.now().toIso8601String();

  // 1. Set Storage permissions.
  //    The file was just uploaded from MAIA with [] (no permissions), so we
  //    build the list from scratch – no need to fetch and merge first.
  final storagePerms = <String>[
    for (final id in targetAccountIds) 'read("user:$id")',
    'read("team:admin")',
    'update("team:admin")',
    'delete("team:admin")',
  ];
  try {
    await storage.updateFile(
      bucketId: mediaFilesBucketId,
      fileId: storageFileId,
      permissions: storagePerms,
    );
    context.log(
      'Storage permissions set for ${targetAccountIds.length} users',
    );
  } catch (e) {
    // Log but don't abort – DB records are still useful even if storage
    // permission update fails (e.g. SDK response-parsing quirks in v21).
    context.log('Warning: storage.updateFile failed: $e');
  }

  // 2. Create mediaFiles DB document (read-only for all recipients, no owner)
  final dbPerms = <String>[
    for (final id in targetAccountIds) 'read("user:$id")',
    'read("team:admin")',
    'update("team:admin")',
    'delete("team:admin")',
  ];
  final mediaFileDoc = await database.createRow(
    databaseId: databaseId,
    tableId: mediaFilesCollectionId,
    rowId: appwrite.ID.unique(),
    data: {
      'fileName': fileName,
      'originalName': fileName,
      'fileType': 'video',
      'mimeType': mimeType,
      'fileSizeBytes': fileSizeBytes,
      'uploadedBy': adminUserId.isNotEmpty ? adminUserId : 'broadcast-system',
      'uploadedAt': now,
      'description': 'Broadcast-Video (Broadcast: $broadcastDocId)',
      'durationSeconds': 0,
      'storageFileId': storageFileId,
      'folder': 'Broadcasts',
    },
    permissions: dbPerms,
  );
  final mediaFileDocId = mediaFileDoc.$id;
  context.log('MediaFiles record created: $mediaFileDocId');

  // 3. Create file_access 'read' entry per recipient (no 'owner' – Option B)
  int created = 0;
  for (final accountId in targetAccountIds) {
    try {
      await database.createRow(
        databaseId: databaseId,
        tableId: fileAccessCollectionId,
        rowId: appwrite.ID.unique(),
        data: {
          'fileId': mediaFileDocId,
          'userId': accountId,
          'accessType': 'read',
          'grantedAt': now,
          'grantedBy': adminUserId.isNotEmpty ? adminUserId : null,
        },
        permissions: [
          'read("user:$accountId")',
          'update("user:$accountId")',
          'delete("user:$accountId")',
          'read("team:admin")',
          'update("team:admin")',
          'delete("team:admin")',
        ],
      );
      created++;
    } catch (e) {
      context.log('Warning: file_access for $accountId failed: $e');
    }
  }
  context.log('Created $created file_access entries');
  return created;
}

// ═════════════════════════════════════════════════════════════════════════════
// UPDATE
// ═════════════════════════════════════════════════════════════════════════════
Future<dynamic> _handleUpdate(
  dynamic context,
  appwrite.TablesDB database,
  String databaseId,
  String broadcastCollectionId,
  Map<String, dynamic> body,
) async {
  final documentId = (body['documentId'] as String?)?.trim() ?? '';

  if (documentId.isEmpty) {
    return context.res.json({
      'ok': false,
      'error': '"documentId" is required for update',
    }, 400);
  }

  final updateData = <String, dynamic>{};

  if (body.containsKey('text')) {
    updateData['text'] = (body['text'] as String?)?.trim() ?? '';
  }
  if (body.containsKey('priority')) {
    // Normalize to uppercase
    updateData['priority'] =
        ((body['priority'] as String?)?.trim() ?? 'normal').toUpperCase();
  }
  if (body.containsKey('videoUrl')) {
    updateData['video_url'] = body['videoUrl'] as String?;
  }
  if (body.containsKey('storageFileId')) {
    final sfi = (body['storageFileId'] as String?)?.trim();
    if (sfi != null && sfi.isNotEmpty) {
      // Encode as storage:// URI so Flutter can distinguish from plain URLs
      updateData['video_url'] = 'storage://$sfi';
    }
  }
  if (body.containsKey('isActive')) {
    updateData['is_active'] = body['isActive'] as bool? ?? true;
  }

  if (updateData.isEmpty) {
    return context.res.json({'ok': false, 'error': 'No fields to update'}, 400);
  }

  context.log(
    'Updating broadcast $documentId with ${updateData.keys.join(', ')}',
  );

  final doc = await database.updateRow(
    databaseId: databaseId,
    tableId: broadcastCollectionId,
    rowId: documentId,
    data: updateData,
  );

  context.log('Broadcast updated: ${doc.$id}');

  return context.res.json({
    'ok': true,
    'result': 'Broadcast updated',
    'document': {
      '\$id': doc.$id,
      '\$createdAt': doc.$createdAt,
      '\$updatedAt': doc.$updatedAt,
      ...doc.data,
    },
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// DELETE
// ═════════════════════════════════════════════════════════════════════════════
Future<dynamic> _handleDelete(
  dynamic context,
  appwrite.TablesDB database,
  String databaseId,
  String broadcastCollectionId,
  Map<String, dynamic> body,
) async {
  final documentId = (body['documentId'] as String?)?.trim() ?? '';

  if (documentId.isEmpty) {
    return context.res.json({
      'ok': false,
      'error': '"documentId" is required for delete',
    }, 400);
  }

  context.log('Deleting broadcast $documentId');

  await database.deleteRow(
    databaseId: databaseId,
    tableId: broadcastCollectionId,
    rowId: documentId,
  );

  context.log('Broadcast deleted: $documentId');

  return context.res.json({
    'ok': true,
    'result': 'Broadcast $documentId deleted',
  });
}
