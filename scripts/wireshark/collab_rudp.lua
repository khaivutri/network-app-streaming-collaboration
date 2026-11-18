-- Wireshark Lua Dissector for Hybrid RUDP / P2P Collaboration Protocol
-- Matches pkg/protocol/constants.go, pkg/protocol/header.go, and pkg/crypto/aead_gcm.go

local band = (bit and bit.band) or (bit32 and bit32.band)

local collab = Proto("collab_rudp", "Collab RUDP / P2P Protocol")

local msg_names = {
    [0x00] = "CTRL_HANDSHAKE",
    [0x10] = "AUTH_REQUEST",
    [0x11] = "AUTH_RESPONSE",
    [0x12] = "LOGOUT",
    [0x13] = "LOGOUT_RESPONSE",
    [0x20] = "DISCO_PEERS",
    [0x21] = "DISCO_PEERS_RESPONSE",
    [0x22] = "DISCO_UPDATE",
    [0x30] = "MESS_BROADCAST",
    [0x31] = "MESS_UNICAST",
    [0x32] = "MESS_DELIVERED",
    [0x40] = "FILE_INIT",
    [0x41] = "FILE_RESPONSE",
    [0x42] = "FILE_CHUNK",
    [0x43] = "FILE_COMPLETE",
    [0x44] = "FILE_CANCEL",
    [0x50] = "ACK",
    [0x51] = "PING",
    [0x52] = "PONG",
    [0x53] = "WINDOW_UPDATE",
    [0x54] = "ERROR",
    [0x60] = "PEER_HELLO",
    [0x61] = "PEER_HELLO_ACK",
    [0x62] = "PEER_BYE",
}

local flag_defs = {
    { 0x01, "SYN" },
    { 0x02, "ACK" },
    { 0x04, "FIN" },
    { 0x08, "RST" },
    { 0x10, "REL" },
    { 0x20, "ORD" },
    { 0x40, "FRAG" },
    { 0x80, "ENC" },
}

local function format_flags(flags)
    if flags == 0 then
        return "NONE"
    end
    local parts = {}
    for _, item in ipairs(flag_defs) do
        if band(flags, item[1]) ~= 0 then
            parts[#parts + 1] = item[2]
        end
    end
    return table.concat(parts, "|")
end

local function stream_label(stream_id)
    if stream_id == 0x00 then
        return "Control"
    elseif stream_id == 0x01 then
        return "BroadcastChat"
    elseif stream_id == 0x02 then
        return "DirectChat"
    elseif stream_id >= 0x03 and stream_id <= 0xFE then
        return "FileTransfer"
    end
    return "Reserved"
end

-- Protocol fields (display filters: collab.*)
local f_version       = ProtoField.uint8("collab.version", "Version", base.HEX)
local f_flags         = ProtoField.uint8("collab.flags", "Flags", base.HEX)
local f_flag_syn      = ProtoField.bool("collab.flags.syn", "SYN", 8, nil, 0x01)
local f_flag_ack      = ProtoField.bool("collab.flags.ack", "ACK", 8, nil, 0x02)
local f_flag_fin      = ProtoField.bool("collab.flags.fin", "FIN", 8, nil, 0x04)
local f_flag_rst      = ProtoField.bool("collab.flags.rst", "RST", 8, nil, 0x08)
local f_flag_rel      = ProtoField.bool("collab.flags.rel", "REL (Reliable)", 8, nil, 0x10)
local f_flag_ord      = ProtoField.bool("collab.flags.ord", "ORD (Ordered)", 8, nil, 0x20)
local f_flag_frag     = ProtoField.bool("collab.flags.frag", "FRAG (Fragment)", 8, nil, 0x40)
local f_flag_enc      = ProtoField.bool("collab.flags.enc", "ENC (AES-256-GCM)", 8, nil, 0x80)
local f_msg_type      = ProtoField.uint8("collab.msg_type", "Message Type", base.HEX, msg_names)
local f_stream_id     = ProtoField.uint8("collab.stream_id", "Stream ID", base.DEC)
local f_conn_id       = ProtoField.uint32("collab.conn_id", "Connection ID", base.HEX)
local f_seq_num       = ProtoField.uint32("collab.seq", "Sequence Number", base.DEC)
local f_ack_num       = ProtoField.uint32("collab.ack", "Acknowledgment Number", base.DEC)
local f_ack_bitmap    = ProtoField.uint16("collab.ack_bitmap", "SACK Bitmap", base.HEX)
local f_window_size   = ProtoField.uint16("collab.window", "Window Size", base.DEC)
local f_nonce         = ProtoField.bytes("collab.nonce", "AES-GCM Nonce (12B)")
local f_nonce_dir     = ProtoField.string("collab.nonce.dir", "Direction Prefix")
local f_nonce_counter = ProtoField.uint64("collab.nonce.counter", "Nonce Counter", base.DEC)
local f_ciphertext    = ProtoField.bytes("collab.ciphertext", "AES-256-GCM Ciphertext")
local f_gcm_tag       = ProtoField.bytes("collab.gcm_tag", "AES-256-GCM Auth Tag (16B)")
local f_payload_plain = ProtoField.bytes("collab.payload", "Plaintext Payload")

collab.fields = {
    f_version, f_flags,
    f_flag_syn, f_flag_ack, f_flag_fin, f_flag_rst,
    f_flag_rel, f_flag_ord, f_flag_frag, f_flag_enc,
    f_msg_type, f_stream_id, f_conn_id,
    f_seq_num, f_ack_num, f_ack_bitmap, f_window_size,
    f_nonce, f_nonce_dir, f_nonce_counter, f_ciphertext, f_gcm_tag,
    f_payload_plain,
}

local json_dissector = Dissector.get("json")

function collab.dissector(tvb, pinfo, tree)
    local total_len = tvb:len()
    if total_len < 20 then
        return 0
    end

    local version = tvb(0, 1):uint()
    if version ~= 0x01 then
        return 0
    end

    local flags     = tvb(1, 1):uint()
    local msg_type  = tvb(2, 1):uint()
    local stream_id = tvb(3, 1):uint()
    local conn_id   = tvb(4, 4):uint()
    local seq_num   = tvb(8, 4):uint()
    local ack_num   = tvb(12, 4):uint()
    local ack_bmp   = tvb(16, 2):uint()
    local win_size  = tvb(18, 2):uint()

    local msg_name   = msg_names[msg_type] or string.format("UNKNOWN(0x%02X)", msg_type)
    local flags_str  = format_flags(flags)
    local stream_str = stream_label(stream_id)
    local is_enc     = band(flags, 0x80) ~= 0

    local is_p2p = (pinfo.src_port ~= 9000 and pinfo.dst_port ~= 9000)
    local mode_tag = is_p2p and "P2P" or "SRV"

    pinfo.cols.protocol = "COLLAB-RUDP"
    local info = string.format("[%s] %s [%s] Stream=%d(%s) Conn=0x%08X",
        mode_tag, msg_name, flags_str, stream_id, stream_str, conn_id)
    if seq_num > 0 then
        info = info .. string.format(" Seq=%u", seq_num)
    end
    if ack_num > 0 then
        info = info .. string.format(" Ack=%u", ack_num)
    end
    if ack_bmp ~= 0 then
        info = info .. string.format(" SACK=0x%04X", ack_bmp)
    end
    if msg_type == 0x50 then
        info = info .. string.format(" Win=%u", win_size)
    end
    if total_len > 20 then
        local payload_len = is_enc and math.max(0, total_len - 48) or (total_len - 20)
        info = info .. string.format(" Len=%dB", payload_len)
    end
    pinfo.cols.info = info

    local subtree = tree:add(collab, tvb(), string.format("Collab RUDP (%s - %s)", mode_tag, msg_name))
    local hdr_tree = subtree:add(collab, tvb(0, 20), "Header (20 bytes, AAD Plaintext)")
    hdr_tree:add(f_version, tvb(0, 1))

    local flags_item = hdr_tree:add(f_flags, tvb(1, 1))
    flags_item:append_text(" (" .. flags_str .. ")")
    flags_item:add(f_flag_enc, tvb(1, 1))
    flags_item:add(f_flag_frag, tvb(1, 1))
    flags_item:add(f_flag_ord, tvb(1, 1))
    flags_item:add(f_flag_rel, tvb(1, 1))
    flags_item:add(f_flag_rst, tvb(1, 1))
    flags_item:add(f_flag_fin, tvb(1, 1))
    flags_item:add(f_flag_ack, tvb(1, 1))
    flags_item:add(f_flag_syn, tvb(1, 1))

    hdr_tree:add(f_msg_type, tvb(2, 1))
    local s_item = hdr_tree:add(f_stream_id, tvb(3, 1))
    s_item:append_text(" (" .. stream_str .. ")")
    hdr_tree:add(f_conn_id, tvb(4, 4))
    hdr_tree:add(f_seq_num, tvb(8, 4))
    hdr_tree:add(f_ack_num, tvb(12, 4))
    hdr_tree:add(f_ack_bitmap, tvb(16, 2))
    hdr_tree:add(f_window_size, tvb(18, 2))

    if total_len == 20 then
        return total_len
    end

    if is_enc and total_len >= 48 then
        local enc_tree = subtree:add(collab, tvb(20, total_len - 20),
            string.format("AES-256-GCM Body (%d bytes)", total_len - 20))
        local nonce_tree = enc_tree:add(f_nonce, tvb(20, 12))
        local prefix_raw = tvb(20, 3):string()
        local dir_desc = prefix_raw
        if prefix_raw == "C2S" then
            dir_desc = "C2S (Initiator -> Responder)"
        elseif prefix_raw == "S2C" then
            dir_desc = "S2C (Responder -> Initiator)"
        end
        nonce_tree:add(f_nonce_dir, tvb(20, 3)):set_text("Direction Prefix: " .. dir_desc)
        nonce_tree:add(f_nonce_counter, tvb(24, 8))

        local cipher_len = total_len - 48
        if cipher_len > 0 then
            enc_tree:add(f_ciphertext, tvb(32, cipher_len))
        end
        enc_tree:add(f_gcm_tag, tvb(total_len - 16, 16))
    else
        local payload_tvb = tvb(20, total_len - 20)
        if json_dissector ~= nil and tvb(20, 1):uint() == 0x7B then
            json_dissector:call(payload_tvb:tvb(), pinfo, subtree)
            -- Restore protocol/info columns in case JSON dissector overwrote them
            pinfo.cols.protocol = "COLLAB-RUDP"
            pinfo.cols.info = info
        else
            subtree:add(f_payload_plain, payload_tvb)
        end
    end

    return total_len
end

-- Heuristic detector so random ephemeral P2P ports (--p2p-addr 0.0.0.0:0) also dissect automatically
local function heur_dissect(tvb, pinfo, tree)
    local total_len = tvb:len()
    if total_len < 20 then
        return false
    end
    if tvb(0, 1):uint() ~= 0x01 then
        return false
    end
    local flags = tvb(1, 1):uint()
    local msg_type = tvb(2, 1):uint()
    if msg_names[msg_type] == nil then
        return false
    end

    if band(flags, 0x80) ~= 0 then
        if total_len < 48 then
            return false
        end
        local dir_prefix = tvb(20, 3):string()
        local dir_nul = tvb(23, 1):uint()
        if (dir_prefix ~= "C2S" and dir_prefix ~= "S2C") or dir_nul ~= 0 then
            return false
        end
    else
        if total_len > 20 and tvb(20, 1):uint() ~= 0x7B then
            return false
        end
    end

    collab.dissector(tvb, pinfo, tree)
    return true
end

local udp_table = DissectorTable.get("udp.port")
udp_table:add(9000, collab)
for port = 9101, 9105 do
    udp_table:add(port, collab)
end

pcall(function()
    collab:register_heuristic("udp", heur_dissect)
end)
