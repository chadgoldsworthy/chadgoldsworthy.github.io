SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [stage].[load_rl_dim_all_fund_attributes] 
    @BatchId [varchar](50), @IsAtomic [bit], @FileName [varchar](250), @Provider [varchar](100), @DataSource [varchar](100), @ArchiveFileName [varchar](200) AS

BEGIN
    -- Status: 1 passed, 2 failed, 3 warning
	-- Action: 1 insert, 2 update
    DECLARE @InvalidCount int = 0, @Status smallint = 1, @Added int = 0, @Updated int = 0, @Message varchar(1000) = 'Load passed';
	DECLARE @Updates TABLE
	(
		StageId bigint,
		StoreId bigint
	);
    DECLARE @Stage TABLE
	(
		StageId bigint
	);

    BEGIN TRY
        SELECT @InvalidCount = COUNT(*) FROM [stage].[rl_dim_all_fund_attributes] WHERE BatchId = @BatchId AND IsValid = 0;
		IF @InvalidCount > 0 AND @IsAtomic = 1
        BEGIN 
            THROW 50100, 'IsAtomic is true and there are invalid records', 200;
        END
        IF @InvalidCount > 0
        BEGIN 
            SET @Status = 3
            SET @Message = 'Load passed but some invalid records'
        END

        INSERT INTO @Stage (StageId)
		SELECT MAX(Id) FROM [stage].[rl_dim_all_fund_attributes]  
		WHERE BatchId = @BatchId AND IsValid = 1
		GROUP BY FundId;

		UPDATE f SET 
            f.BatchId = s.BatchId,
            f.Action = 2, 
            f.Status = COALESCE(s.Status, f.Status),
            f.FundCommencementDate = COALESCE(s.FundCommencementDate, f.FundCommencementDate),
            f.FundCategory = COALESCE(s.FundCategory, f.FundCategory),
            f.FundFamilyNumber = COALESCE(s.FundFamilyNumber, f.FundFamilyNumber),
            f.FundFamilyDescription = COALESCE(s.FundFamilyDescription, f.FundFamilyDescription),
            f.FundType = COALESCE(s.FundType, f.FundType),
            f.AssetManagerNumber = COALESCE(s.AssetManagerNumber, f.AssetManagerNumber),
            f.Reg28Compliant = COALESCE(s.Reg28Compliant, f.Reg28Compliant),
            f.FundRangeOption = COALESCE(s.FundRangeOption, f.FundRangeOption),
            f.BDANumber = COALESCE(s.BDANumber, f.BDANumber),
            f.PriceSource = COALESCE(s.PriceSource, f.PriceSource),
            f.TotalExpenseRatio = COALESCE(s.TotalExpenseRatio, f.TotalExpenseRatio),
            f.AssetManagerDescription = COALESCE(s.AssetManagerDescription, f.AssetManagerDescription),
            f.HashKey = COALESCE(s.HashKey, f.HashKey)
		FROM [stage].[rl_dim_all_fund_attributes] s 
        INNER JOIN @Stage st
		ON st.StageId = s.Id
		INNER JOIN [store].[rl_dim_all_fund_attributes] f
		ON f.FundId = s.FundId AND f.StartDate = s.EffectiveDate AND f.HashKey != s.HashKey
		WHERE s.BatchId = @BatchId AND s.IsValid = 1;

		INSERT INTO @Updates (StageId, StoreId)
		SELECT s.Id as StageId, f.Id as StoreId 
		FROM [stage].[rl_dim_all_fund_attributes] s  
        INNER JOIN @Stage st
		ON st.StageId = s.Id
		INNER JOIN [store].[rl_dim_all_fund_attributes] f
		ON f.FundId = s.FundId AND f.StartDate < s.EffectiveDate AND f.EndDate >= s.EffectiveDate AND (s.HashKey != f.HashKey OR f.HashKey IS NULL)
		WHERE s.BatchId = @BatchId AND s.IsValid = 1;
		
		-- Insert new record with the end date of the record it just split
		INSERT INTO [store].[rl_dim_all_fund_attributes] 
		([BatchId], [Action], [FundId], [StartDate], [EndDate], [Provider], [DataSource], [Status], [FundCommencementDate], 
		[FundCategory], [FundFamilyNumber], [FundFamilyDescription], [FundType], [AssetManagerNumber], [Reg28Compliant], 
		[FundRangeOption], [BDANumber], [PriceSource], [TotalExpenseRatio], [AssetManagerDescription], [HashKey]) 

		SELECT 
            @BatchId, 1, s.FundId, s.EffectiveDate as StartDate, f.EndDate as EndDate, 
            COALESCE(s.Provider, f.Provider),
            COALESCE(s.DataSource, f.DataSource),
            COALESCE(s.Status, f.Status),
            COALESCE(s.FundCommencementDate, f.FundCommencementDate),
            COALESCE(s.FundCategory, f.FundCategory),
            COALESCE(s.FundFamilyNumber, f.FundFamilyNumber),
            COALESCE(s.FundFamilyDescription, f.FundFamilyDescription),
            COALESCE(s.FundType, f.FundType),
            COALESCE(s.AssetManagerNumber, f.AssetManagerNumber),
            COALESCE(s.Reg28Compliant, f.Reg28Compliant),
            COALESCE(s.FundRangeOption, f.FundRangeOption),
            COALESCE(s.BDANumber, f.BDANumber),
            COALESCE(s.PriceSource, f.PriceSource),
            COALESCE(s.TotalExpenseRatio, f.TotalExpenseRatio),
            COALESCE(s.AssetManagerDescription, f.AssetManagerDescription),
            COALESCE(s.HashKey, f.HashKey)
		FROM @Updates u 
		INNER JOIN [stage].[rl_dim_all_fund_attributes] s
		ON s.Id = u.StageId
		INNER JOIN [store].[rl_dim_all_fund_attributes] f
		ON f.Id = u.StoreId;

		-- Update the split record by setting the end date to a day before the effective date of the new record
		UPDATE f SET 
		f.BatchId = s.BatchId,
		f.Action = 2, 
		f.EndDate = DATEADD(day, -1, s.EffectiveDate)
		
		FROM @Updates u 
		INNER JOIN [stage].[rl_dim_all_fund_attributes] s
		ON s.Id = u.StageId
		INNER JOIN [store].[rl_dim_all_fund_attributes] f
		ON f.Id = u.StoreId;

		-- Insert records where they dont exist at all
		INSERT INTO [store].[rl_dim_all_fund_attributes] 
		([BatchId], [Action], [FundId], [StartDate], [EndDate], [Provider], [DataSource], [Status], [FundCommencementDate], 
		[FundCategory], [FundFamilyNumber], [FundFamilyDescription], [FundType], [AssetManagerNumber], [Reg28Compliant], 
		[FundRangeOption], [BDANumber], [PriceSource], [TotalExpenseRatio], [AssetManagerDescription], [HashKey]) 
		
		SELECT 
            @BatchId, 1, s.FundId, '1753-01-01' as StartDate, '9999-12-31' as EndDate,
            s.Provider,s.DataSource,s.Status,s.FundCommencementDate,s.FundCategory,s.FundFamilyNumber,s.FundFamilyDescription,s.FundType,
            s.AssetManagerNumber,s.Reg28Compliant,s.FundRangeOption,s.BDANumber,s.PriceSource,s.TotalExpenseRatio,
            s.AssetManagerDescription,s.HashKey
		FROM [stage].[rl_dim_all_fund_attributes] s
        INNER JOIN @Stage st
		ON st.StageId = s.Id
		LEFT OUTER JOIN [store].[rl_dim_all_fund_attributes] f
		ON s.FundId = f.FundId
		WHERE s.BatchId = @BatchId AND s.IsValid = 1 AND f.FundId IS NULL;
		
        SELECT @Added = COUNT(*) FROM [store].[rl_dim_all_fund_attributes] WHERE BatchId = @BatchId AND Action = 1;
		SELECT @Updated = COUNT(*) FROM [store].[rl_dim_all_fund_attributes] WHERE BatchId = @BatchId AND Action = 2;

    END TRY

    BEGIN CATCH
        SET @Message = LEFT(ERROR_MESSAGE(), 1000);
        SET @Status = 2;
    END CATCH
    
    EXEC [audit].add_load_log @BatchId, @FileName, @Provider, @DataSource, 'store.dim_all_fund_attributes', @Status, @Added, @Updated, @Message, @ArchiveFileName;

    DELETE FROM [stage].[rl_dim_all_fund_attributes] WHERE BatchId = @BatchId;

	EXEC [audit].[raise_error] @Status, @Message, @InvalidCount;
END
GO