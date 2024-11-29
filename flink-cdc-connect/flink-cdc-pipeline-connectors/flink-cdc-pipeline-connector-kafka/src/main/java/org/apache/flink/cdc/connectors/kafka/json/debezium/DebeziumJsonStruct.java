package org.apache.flink.cdc.connectors.kafka.json.debezium;

/** Debezium JSON struct. */
public class DebeziumJsonStruct {

    enum DebeziumStruct {
        SCHEMA(0, "schema"),
        PAYLOAD(1, "payload");

        private final int position;
        private final String fieldName;

        DebeziumStruct(int position, String fieldName) {
            this.position = position;
            this.fieldName = fieldName;
        }

        public int getPosition() {
            return position;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    enum DebeziumPayload {
        BEFORE(0, "before"),
        AFTER(1, "after"),
        OPERATION(2, "op"),
        SOURCE(3, "source");

        private final int position;
        private final String fieldName;

        DebeziumPayload(int position, String fieldName) {
            this.position = position;
            this.fieldName = fieldName;
        }

        public int getPosition() {
            return position;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    enum DebeziumSource {
        DATABASE(0, "db"),
        TABLE(1, "table");

        private final int position;
        private final String fieldName;

        DebeziumSource(int position, String fieldName) {
            this.position = position;
            this.fieldName = fieldName;
        }

        public int getPosition() {
            return position;
        }

        public String getFieldName() {
            return fieldName;
        }
    }
}
