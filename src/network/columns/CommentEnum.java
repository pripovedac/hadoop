package network.columns;

public enum CommentEnum {
    CREATIION_DATE(0),
    COMMENT_ID(1),
    CREATOR_ID(2),
    COMMENT(3),
    CREATOR_NAME(4),
    PARENT_ID(5);

    private int _value;

    private CommentEnum(int value) {
        this._value = value;
    }

    private int getValue() {
        return _value;
    }
}
