import 'dart:async';
import 'package:flutter/material.dart';
import 'package:aws_kinesis_api/kinesis-2013-12-02.dart';
import 'package:aws_common/aws_common.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Kinesis Reader',
      theme: ThemeData(
        primarySwatch: Colors.blue,
      ),
      home: const KinesisReaderPage(),
    );
  }
}

class KinesisReaderPage extends StatefulWidget {
  const KinesisReaderPage({Key? key}) : super(key: key);

  @override
  _KinesisReaderPageState createState() => _KinesisReaderPageState();
}

class _KinesisReaderPageState extends State<KinesisReaderPage> {
  late Kinesis _client;
  late StreamController<List<String>> _streamController;
  bool _isReading = false;

  @override
  void initState() {
    super.initState();
    _initializeKinesisClient();
    _streamController = StreamController<List<String>>.broadcast();
  }

  @override
  void dispose() {
    _streamController.close();
    super.dispose();
  }

  void _initializeKinesisClient() {
    final credentials = AwsClientCredentials(
      accessKey: '',
      secretKey: '',
    );
    const region = 'us-east-1'; // Replace with your AWS region

    _client = Kinesis(
      region: region,
      credentials: credentials,
    );
  }

  Future<void> _startReading() async {
    if (_isReading) return;

    setState(() {
      _isReading = true;
    });

    const streamName = 'mqtt-test';

    try {
      final describeStreamResponse = await _client.describeStream(
        streamName: streamName,
      );

      final shardId = describeStreamResponse.streamDescription.shards[3].shardId;
      
      final getShardIteratorResponse = await _client.getShardIterator(
        streamName: streamName,
        shardId: shardId,
        shardIteratorType: ShardIteratorType.trimHorizon,
      );

      var shardIterator = getShardIteratorResponse.shardIterator;

      while (_isReading) {
        final getRecordsResponse = await _client.getRecords(
          shardIterator: shardIterator!,
          limit: 100,
        );

        final records = getRecordsResponse.records;

        if (records.isNotEmpty) {
          final newRecords = records.map((record) =>
            'Seq: ${record.sequenceNumber}, '
            'Key: ${record.partitionKey}, '
            'Data: ${String.fromCharCodes(record.data)}'
          ).toList();

          _streamController.add(newRecords);
        }

        shardIterator = getRecordsResponse.nextShardIterator;
        await Future.delayed(const Duration(seconds: 1));
      }
    } catch (e) {
      print('Error: $e');
      _stopReading();
    }
  }

  void _stopReading() {
    setState(() {
      _isReading = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Kinesis Reader'),
      ),
      body: Column(
        children: [
          Expanded(
            child: StreamBuilder<List<String>>(
              stream: _streamController.stream,
              builder: (context, snapshot) {
                if (snapshot.hasError) {
                  return Center(child: Text('Error: ${snapshot.error}'));
                }

                if (!snapshot.hasData) {
                  return const Center(child: Text('No data yet'));
                }

                final allRecords = snapshot.data!;

                return ListView.builder(
                  itemCount: allRecords.length,
                  itemBuilder: (context, index) {
                    final reversedIndex = allRecords.length - 1 - index;
                    return ListTile(
                      title: Text(allRecords[reversedIndex]),
                    );
                  },
                );
              },
            ),
          ),
          Padding(
            padding: const EdgeInsets.all(8.0),
            child: ElevatedButton(
              onPressed: _isReading ? _stopReading : _startReading,
              child: Text(_isReading ? 'Stop Reading' : 'Start Reading'),
            ),
          ),
        ],
      ),
    );
  }
}
