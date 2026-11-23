package io.pulsemq.protocol;

import io.netty.buffer.ByteBuf;
import io.pulsemq.common.ErrorCode;

import java.util.ArrayList;
import java.util.List;

public record JoinGroupRequest(
        RequestHeader header,
        String groupId,
        String memberId,
        String protocolType,
        List<String> groupProtocols
) implements PulseRequest {

    @Override
    public void writeBody(ByteBuf out) {
        OffsetCommitRequest.writeString(out, groupId);
        OffsetCommitRequest.writeString(out, memberId);
        OffsetCommitRequest.writeString(out, protocolType);

        out.writeInt(groupProtocols.size());
        for (String proto : groupProtocols) {
            OffsetCommitRequest.writeString(out, proto);
        }
    }

    public static JoinGroupRequest readFrom(RequestHeader header, ByteBuf in) {
        String groupId = OffsetCommitRequest.readString(in);
        String memberId = OffsetCommitRequest.readString(in);
        String protocolType = OffsetCommitRequest.readString(in);

        int protoCount = in.readInt();
        List<String> protos = new ArrayList<>(protoCount);
        for (int i = 0; i < protoCount; i++) {
            protos.add(OffsetCommitRequest.readString(in));
        }

        return new JoinGroupRequest(header, groupId, memberId, protocolType, protos);
    }
}
