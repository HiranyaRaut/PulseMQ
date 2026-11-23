package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;

import java.util.ArrayList;
import java.util.List;

public record JoinGroupResponse(
        ResponseHeader header,
        ErrorCode errorCode,
        int generationId,
        String memberId,
        String leaderId,
        List<GroupMember> members
) implements PulseResponse {

    public record GroupMember(String memberId) {}

    @Override
    public void writeBody(ByteBuf out) {
        out.writeShort(errorCode.code());
        out.writeInt(generationId);
        OffsetCommitRequest.writeString(out, memberId);
        OffsetCommitRequest.writeString(out, leaderId);

        out.writeInt(members.size());
        for (GroupMember m : members) {
            OffsetCommitRequest.writeString(out, m.memberId());
        }
    }

    public static JoinGroupResponse readFrom(ResponseHeader header, ByteBuf in) {
        short err = in.readShort();
        int gen = in.readInt();
        String memberId = OffsetCommitRequest.readString(in);
        String leaderId = OffsetCommitRequest.readString(in);

        int count = in.readInt();
        List<GroupMember> members = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            members.add(new GroupMember(OffsetCommitRequest.readString(in)));
        }

        return new JoinGroupResponse(header, ErrorCode.fromCode(err), gen, memberId, leaderId, members);
    }
}
