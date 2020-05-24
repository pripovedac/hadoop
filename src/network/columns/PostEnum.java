package network.columns;

public enum PostEnum {
    CREATIION_DATE(0),
    POST_ID(1),
    CREATOR_ID(2),
    POST(3),
    CREATOR_NAME(4);

    private int _value;

    private PostEnum(int value) {
        this._value = value;
    }

    private int getValue() {
        return _value;
    }
}
