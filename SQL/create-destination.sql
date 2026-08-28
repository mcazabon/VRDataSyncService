/*
    Destination database schema (VF_AMR_RP1) for the section data migration.

    Creates the database used by ISAMU_SQL,1433 and the four dbo tables
    described in the migration specification. section_centera2 has a
    destination identity key and both child tables have section_start_time.
    Constraints are intentionally omitted because no definitions were supplied.
*/
USE [master];
GO

IF DB_ID(N'VF_AMR_RP1') IS NULL
BEGIN
    CREATE DATABASE [VF_AMR_RP1];
END;
GO

USE [VF_AMR_RP1];
GO

IF OBJECT_ID(N'dbo.section2', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.section2
    (
        CCDR_ID char(36) NOT NULL,
        Location varchar(255) NULL,
        Cause_ID int NULL,
        Audio_Codec_ID int NULL,
        Video_Codec_ID int NULL,
        Direction_ID int NULL,
        FileFormat_ID int NULL,
        Source_IP varchar(64) NULL,
        Source_Caller_ID varchar(900) NULL,
        Source_Name nvarchar(4000) NULL,
        Destination_IP varchar(64) NULL,
        Destination_Caller_ID varchar(900) NULL,
        Destination_Name nvarchar(4000) NULL,
        Start_Time datetime NULL,
        End_Time datetime NULL,
        Url varchar(256) NULL,
        Archive_status bit NULL,
        Keep bit NULL,
        Conference bit NULL,
        Transmitted bit NULL,
        Source_User_ID int NULL,
        Destination_User_ID int NULL,
        ondemand bit NULL,
        Native_ID varchar(512) NULL,
        Owner_EID char(4) NULL,
        storage_server varchar(128) NULL,
        storage_status tinyint NULL,
        inserted_by_trigger bit NULL,
        recorder_service_id int NULL,
        signaling_id int NULL,
        agent_id nvarchar(255) NULL,
        recorded_extension varchar(128) NULL,
        dialed_extension varchar(128) NULL,
        location_eid char(4) NULL,
        platform_call_id nvarchar(127) NULL,
        silence_percent decimal(3, 0) NULL,
        talkover_percent decimal(3, 0) NULL,
        longest_silence_length decimal(10, 0) NULL,
        meeting_id varchar(128) NULL,
        recording_id varchar(64) NULL,
        manual int NULL,
        media_delete_date datetime NULL,
        exists_related_call bit NULL,
        source_audio_rtp_count int NULL,
        destination_audio_rtp_count int NULL,
        encryption_id int NULL,
        signature_id int NULL,
        do_not_delete numeric(1, 0) NULL,
        secondary bit NULL,
        source_device_id nvarchar(1024) NULL,
        destination_device_id nvarchar(1024) NULL,
        recorded_party bit NULL,
        notified bit NULL,
        storage_folder_id int NULL,
        retention_start datetime NULL,
        retention_until datetime NULL,
        retention_auto_delete bit NULL,
        record_failed bit NULL,
        media_error int NULL,
        media_length int NULL,
        source_proxy_ip varchar(64) NULL,
        destination_proxy_ip varchar(64) NULL,
        transcode_date datetime NULL,
        platform_id varchar(16) NULL,
        modality_id varchar(16) NULL,
        voice_quality int NULL,
        forward_reason int NULL,
        import_source_id int NULL,
        user_location varchar(255) NULL,
        cdr_media_type int NULL,
        media_id varchar(256) NULL,
        moved_to_section2 bit NULL,
        deleted bit NULL,
        has_multiple_locations bit NULL,
        verintmig_db_subset_id int NULL,
        verintmig_tar_sha1 bigint NULL,
        priv bit NULL,
        im_end_time_updated bit NULL,
        platform_tenant nvarchar(255) NULL,
        media_time_from datetime NULL,
        media_time_to datetime NULL,
        storage_folder_id2 int NULL,
        retention_start2 datetime NULL,
        retention_until2 datetime NULL,
        retention_auto_delete2 bit NULL,
        ts_insert1 datetime NULL,
        hold tinyint NULL,
        register_hold_on_storage tinyint NULL,
        register_hold_on_storage_last_changed datetime NULL,
        ts_update datetime NULL,
        ts_insert_ongoing datetime NULL,
        storage_policy_ids_processed varchar(1000) NULL,
        storage_policy_error_waiting_hours int NULL,
        task_ids_processed varchar(1000) NULL,
        external_id int NULL
    );
END;
GO

IF OBJECT_ID(N'dbo.section_meta2', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.section_meta2
    (
        moved_to_section_meta2 bit NULL,
        ccdr_id char(36) NULL,
        meta_template_id int NULL,
        section_start_time datetime NULL,
        creationdate datetime NULL,
        createdby int NULL,
        lastmodificationdate datetime NULL,
        lastmodifiedby int NULL,
        value_0 nvarchar(818) NULL,
        value_1 nvarchar(818) NULL,
        value_2 nvarchar(818) NULL,
        value_3 nvarchar(818) NULL,
        value_4 nvarchar(818) NULL,
        value_5 nvarchar(818) NULL,
        value_6 nvarchar(818) NULL,
        value_7 nvarchar(818) NULL,
        value_8 nvarchar(818) NULL,
        value_9 nvarchar(818) NULL,
        value_10 nvarchar(818) NULL,
        value_11 nvarchar(818) NULL,
        value_12 nvarchar(818) NULL,
        value_13 nvarchar(818) NULL,
        value_14 nvarchar(818) NULL,
        value_15 nvarchar(818) NULL,
        value_16 nvarchar(818) NULL,
        value_17 nvarchar(818) NULL,
        value_18 nvarchar(818) NULL,
        value_19 nvarchar(818) NULL,
        value_20 nvarchar(818) NULL,
        value_21 nvarchar(818) NULL,
        value_22 nvarchar(818) NULL,
        value_23 nvarchar(818) NULL,
        value_24 nvarchar(818) NULL,
        value_25 nvarchar(818) NULL,
        value_26 nvarchar(818) NULL,
        value_27 nvarchar(818) NULL,
        value_28 nvarchar(818) NULL,
        value_29 nvarchar(818) NULL,
        value_30 nvarchar(818) NULL,
        value_31 nvarchar(818) NULL,
        value_32 nvarchar(818) NULL,
        value_33 nvarchar(818) NULL,
        value_34 nvarchar(818) NULL,
        value_35 nvarchar(818) NULL,
        value_36 nvarchar(818) NULL,
        value_37 nvarchar(818) NULL,
        value_38 nvarchar(max) NULL,
        value_39 nvarchar(max) NULL,
        ts_insert1 datetime NULL,
        owner_eid_for_trigger char(4) NULL
    );
END;
GO

IF OBJECT_ID(N'dbo.section_centera2', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.section_centera2
    (
        id bigint IDENTITY(1, 1) NOT NULL,
        ccdr_id char(36) NOT NULL,
        extension varchar(32) NULL,
        clip_id varchar(256) NULL,
        moved_to_section_centera2 bit NULL,
        ts_insert1 datetime NULL,
        section_start_time datetime NOT NULL
    );
END;
GO

IF OBJECT_ID(N'dbo.section_cdr_media2', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.section_cdr_media2
    (
        ccdr_id char(36) NOT NULL,
        media_id varchar(256) NULL,
        moved_to_section_cdr_media2 bit NULL,
        one_media_start_time datetime NULL,
        one_media_end_time datetime NULL,
        one_media_ccdr_id char(36) NULL,
        time_from datetime NULL,
        time_to datetime NULL,
        ts_insert1 datetime NULL,
        section_start_time datetime NOT NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.section2') AND name = N'UX_section2_CCDR_ID')
    CREATE UNIQUE CLUSTERED INDEX UX_section2_CCDR_ID ON dbo.section2 (CCDR_ID);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.section2') AND name = N'IX_section2_Start_Time')
    CREATE INDEX IX_section2_Start_Time ON dbo.section2 (Start_Time) INCLUDE (CCDR_ID);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.section_meta2') AND name = N'IX_section_meta2_ccdr_id_section_start_time')
    CREATE INDEX IX_section_meta2_ccdr_id_section_start_time ON dbo.section_meta2 (ccdr_id, section_start_time);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.section_centera2') AND name = N'PK_section_centera2')
    CREATE UNIQUE CLUSTERED INDEX PK_section_centera2 ON dbo.section_centera2 (id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.section_centera2') AND name = N'IX_section_centera2_ccdr_id_section_start_time')
    CREATE INDEX IX_section_centera2_ccdr_id_section_start_time ON dbo.section_centera2 (ccdr_id, section_start_time);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.section_cdr_media2') AND name = N'IX_section_cdr_media2_ccdr_id_section_start_time')
    CREATE INDEX IX_section_cdr_media2_ccdr_id_section_start_time ON dbo.section_cdr_media2 (ccdr_id, section_start_time);
GO