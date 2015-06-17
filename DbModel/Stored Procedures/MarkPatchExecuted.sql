CREATE PROCEDURE [PsDbDeploy].[MarkPatchExecuted]     
        @FilePath [nvarchar](450),
        @CheckSum [nvarchar](100),
        @Comment  [nvarchar](4000)
    AS
    BEGIN
        SET NOCOUNT ON;

        DECLARE @OID bigint

        SELECT @OID=OID
            FROM [PsDbDeploy].[FilePatches]
            WHERE FilePath = @FilePath

        IF  (@@ROWCOUNT = 0)
        BEGIN
            INSERT 
                INTO [PsDbDeploy].[FilePatches]
                    ( [FilePath]
                    , [Applied]
                    , [CheckSum]
                    , [Comment])
            VALUES (@FilePath
                    , GetDate()
                    , @CheckSum
                    , @Comment)
        END
        ELSE BEGIN
            UPDATE [PsDbDeploy].[FilePatches]
                SET CheckSum=@CheckSum
                    , Applied=GetDate()
                    , Comment=@Comment
                WHERE OID=@OID
                AND CheckSum<>@CheckSum
        END
    END