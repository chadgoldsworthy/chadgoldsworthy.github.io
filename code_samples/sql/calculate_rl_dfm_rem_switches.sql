SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [stage].[calculate_rl_dfm_rem_switches] (
    @BatchId uniqueidentifier,
    @Provider varchar(100),
    @DataSource varchar(100),
    @MonthEndDate date
)
AS
BEGIN

    DECLARE @StartDate date, @EndDate date;
    SELECT 
        @StartDate = MIN(FullDate),
        @EndDate = MAX(FullDate)
    FROM [pps-edw-db].reference.calendar
    WHERE [Year] = YEAR(@MonthEndDate)
        AND MonthOfYear = MONTH(@MonthEndDate);

    WITH InvestmentSpecialists AS (
        SELECT DISTINCT
            bcr.StartDate,
            bcr.EndDate,
            bcr.BrokerId,
            u.Id as InvestmentSpecialistId,
            u.[User] as InvestmentSpecialist,
            ISNULL(ur.Channel, 'IS') AS InvestmentSpecialistChannel
        FROM [pps-edw-db].[store].[entity_all_user] u
        INNER JOIN [pps-edw-db].[store].[dist_dim_all_broker_consultant_user_relationship] ur
        ON ur.UserId = u.Id
        INNER JOIN [pps-edw-db].[store].[slx_entity_all_broker_consultant] bc
        ON bc.Id = ur.BrokerConsultantId
        INNER JOIN [pps-edw-db].[store].[slx_dim_all_broker_consultant_broker_relationship] bcr
        ON bcr.BrokerConsultantId = bc.Id
        WHERE bcr.BackRelation LIKE '%Broker Consultant%'
        AND u.Role = 'Investment Specialist'
    ),

    PolicyWrapFundHistory AS (
        SELECT
            p.Id AS PolicyId,
            pa.WrapFundName AS CurrentWrapFundName,
            LAG(pa.WrapFundName) OVER (PARTITION BY p.Id ORDER BY pa.StartDate) AS PreviousWrapFundName,
            pa.StartDate,
            pa.EndDate
        FROM [pps-edw-db].store.entity_all_policy p
        LEFT JOIN [pps-edw-db].store.rl_dim_all_policy_attributes pa
            ON pa.PolicyId = p.Id
        WHERE pa.StartDate < pa.EndDate
    ),

    PolicyPortfolioCodes AS (
        SELECT DISTINCT
            WrapFundName,
            SUBSTRING(WrapFundName, CHARINDEX('(', WrapFundName) +1, CHARINDEX(')', WrapFundName) - CHARINDEX('(', WrapFundName) - 1) AS PortfolioCode
        FROM [pps-edw-db].store.rl_dim_all_policy_attributes
        WHERE WrapFundName LIKE '%(%)%'
        
        UNION ALL
        
        SELECT DISTINCT
            WrapFundName,
            NULL
        FROM [pps-edw-db].store.rl_dim_all_policy_attributes
        WHERE WrapFundName NOT LIKE '%(%)%'
    ),

    WrapFundMapping AS (
        SELECT DISTINCT
            WrapFundName,
            LTRIM(RTRIM(PortfolioCode)) AS PortfolioCode
        FROM PolicyPortfolioCodes
    ),

    WrapFundSwitches AS (
        SELECT DISTINCT
            pwfh.PolicyId,
            pwfh.StartDate AS PortfolioSwitchDate,
            EOMONTH(pwfh.StartDate) AS PortfolioSwitchMonthEndDate,
            pwfh.CurrentWrapFundName,
            wfmc.PortfolioCode AS CurrentPortfolioCode,
            mpfac.FundId AS CurrentFundId,
            mpc.ManagerCode AS CurrentManagerCode,
            pwfh.PreviousWrapFundName,
            wfmp.PortfolioCode AS PreviousPortfolioCode,
            mpp.ManagerCode AS PreviousManagerCode,
            pwfh.StartDate,
            pwfh.EndDate
        FROM PolicyWrapFundHistory pwfh
        LEFT JOIN WrapFundMapping wfmc
            ON wfmc.WrapFundName = pwfh.CurrentWrapFundName
        LEFT JOIN [pps-edw-db].store.entity_all_portfolio mpc
            ON mpc.PortfolioCode = wfmc.PortfolioCode
        LEFT JOIN [pps-edw-db].store.rl_dim_all_model_portfolio_fund_attributes mpfac
            ON mpfac.EntityId = mpc.Id
            AND pwfh.StartDate BETWEEN mpfac.StartDate AND mpfac.EndDate
        LEFT JOIN WrapFundMapping wfmp
            ON wfmp.WrapFundName = pwfh.PreviousWrapFundName
        LEFT JOIN [pps-edw-db].store.entity_all_portfolio mpp
            ON mpp.PortfolioCode = wfmp.PortfolioCode
        WHERE mpc.ManagerCode = '<private>'
            AND (mpp.ManagerCode <> '<private>' OR mpp.ManagerCode IS NULL)
    ),

    Transactions AS (
        SELECT DISTINCT
            tx.PolicyId,
            tx.ProductId,
            tx.FundId,
            tx.UnitHolderId,
            pb.BrokerId,
            ivs.InvestmentSpecialistId,
            ivs.InvestmentSpecialistChannel,
            tx.PriceDate,
            tx.PolicyTransactionNumber,
            tx.TransactionReferenceNumber,
            CASE WHEN fa.AssetManagerNumber = '<private>' THEN 'MM' ELSE 'TP' END AS MMTPSplit,
            tx.GrossAmount,
            tx.NetAmount,
            tt.SubTypeCode
        FROM [pps-edw-db].store.rl_fact_policy_fund_transactions tx
        INNER JOIN [pps-edw-db].reference.rl_transaction_types tt
            ON tt.SubTypeCode = tx.TransactionSubTypeCode
            AND tt.TypeCode = tx.RefType
            AND tt.TransactionTypeCode = tx.TransactionTypeCode
        INNER JOIN [pps-edw-db].store.rl_dim_all_policy_broker_attributes pb
            ON pb.PolicyId = tx.PolicyId
            AND @MonthEndDate BETWEEN pb.StartDate AND pb.EndDate
        LEFT JOIN InvestmentSpecialists ivs
            ON ivs.BrokerId = pb.BrokerId
            AND tx.PriceDate BETWEEN ivs.StartDate AND ivs.EndDate
        LEFT JOIN [pps-edw-db].store.rl_dim_all_fund_attributes fa
            ON fa.FundId = tx.FundId
            AND tx.PriceDate BETWEEN fa.StartDate AND fa.EndDate
        WHERE tt.GroupName = 'Switches'
            AND tt.SubTypeCode = '40A' -- Switch
            AND tx.PriceDate BETWEEN DATEADD(MONTH, -1, @StartDate) AND @EndDate
    ),

    SwitchInTransactions AS (
        SELECT DISTINCT
            tx.PriceDate,
            tx.PolicyId,
            tx.ProductId,
            tx.FundId,
            tx.UnitHolderId,
            tx.BrokerId,
            tx.InvestmentSpecialistId,
            tx.InvestmentSpecialistChannel,
            tx.PolicyTransactionNumber,
            tx.TransactionReferenceNumber,
            wfs.PortfolioSwitchDate,
            wfs.CurrentWrapFundName,
            wfs.CurrentManagerCode,
            tx.MMTPSplit,
            tx.GrossAmount,
            tx.NetAmount
        FROM Transactions tx
        INNER JOIN WrapFundSwitches wfs
            ON wfs.PolicyId = tx.PolicyId
            AND tx.PriceDate BETWEEN wfs.StartDate AND wfs.EndDate
        WHERE tx.NetAmount > 0
    ),

    SwitchOutTransactions AS (
        SELECT DISTINCT
            tx.PriceDate,
            tx.PolicyId,
            tx.PolicyTransactionNumber,
            wfs.PreviousWrapFundName,
            wfs.PreviousManagerCode,
            tx.MMTPSplit,
            tx.GrossAmount,
            tx.NetAmount
        FROM Transactions tx
        INNER JOIN WrapFundSwitches wfs
            ON wfs.PolicyId = tx.PolicyId
            AND tx.PriceDate BETWEEN wfs.StartDate AND wfs.EndDate
        WHERE tx.NetAmount < 0
    )

    INSERT INTO stage.rl_dfm_rem_switches (
        BatchId,
        IsValid,
        [Provider],
        DataSource,
        PriceDate,
        PolicyId,
        ProductId,
        FundId,
        UnitHolderId,
        BrokerId,
        InvestmentSpecialistId,
        InvestmentSpecialistChannel,
        PolicyTransactionNumber,
        TransactionReferenceNumber,
        PortfolioSwitchDate,
        SwitchInWrapFundName,
        SwitchInManagerCode,
        SwitchInGrossAmount,
        SwitchInNetAmount,
        SwitchInMMTPSplit,
        SwitchOutWrapFundName,
        SwitchOutManagerCode,
        SwitchOutGrossAmount,
        SwitchOutNetAmount,
        SwitchOutMMTPSplit,
        SwitchGrossAmount,
        SwitchNetAmount
    )

    SELECT
        @BatchId AS BatchId,
        1 AS IsValid,
        @Provider AS [Provider],
        @DataSource AS DataSource,
        si.PriceDate,
        p2.Id AS PolicyId,
        pr2.Id AS ProductId,
        f2.Id AS FundId,
        uh2.Id AS UnitHolderId,
        b2.Id AS BrokerId,
        ivs2.Id AS InvestmentSpecialistId,
        si.InvestmentSpecialistChannel,
        si.PolicyTransactionNumber,
        si.TransactionReferenceNumber,
        si.PortfolioSwitchDate,
        si.CurrentWrapFundName AS SwitchInWrapFundName,
        si.CurrentManagerCode AS SwitchInManagerCode,
        si.GrossAmount AS SwitchInGrossAmount,
        si.NetAmount AS SwitchInNetAmount,
        si.MMTPSplit AS SwitchInMMTPSplit,
        so.PreviousWrapFundName AS SwitchOutWrapFundName,
        so.PreviousManagerCode AS SwitchOutManagerCode,
        so.GrossAmount AS SwitchOutGrossAmount,
        so.NetAmount AS SwitchOutNetAmount,
        so.MMTPSplit AS SwitchOutMMTPSplit,
        CASE WHEN so.MMTPSplit = 'TP' THEN ABS(so.GrossAmount) ELSE 0 END AS SwitchGrossAmount,
        CASE WHEN so.MMTPSplit = 'TP' THEN ABS(so.NetAmount) ELSE 0 END AS SwitchNetAmount
    FROM SwitchInTransactions si
    INNER JOIN SwitchOutTransactions so
        ON si.PolicyId = so.PolicyId
        AND si.PolicyTransactionNumber = so.PolicyTransactionNumber
        AND si.NetAmount = ABS(so.NetAmount)

    INNER JOIN [pps-edw-db].store.entity_all_policy p
        ON si.PolicyId = p.Id
    INNER JOIN [pps-edw-db].store.entity_all_product pr
        ON si.ProductId = pr.Id
    INNER JOIN [pps-edw-db].store.entity_all_fund f
        ON si.FundId = f.Id
    INNER JOIN [pps-edw-db].store.entity_all_unit_holder uh
        ON si.UnitHolderId = uh.Id
    LEFT JOIN [pps-edw-db].store.entity_all_broker b
        ON si.BrokerId = b.Id
    LEFT JOIN [pps-edw-db].store.entity_all_user ivs
        ON si.InvestmentSpecialistId = ivs.Id
    
    INNER JOIN [pps-distribution-datamart-db].store.entity_all_policy p2
        ON p.PolicyNumber = p2.PolicyNumber
    INNER JOIN [pps-distribution-datamart-db].store.entity_all_product pr2
        ON pr.ProductCode = pr2.ProductCode
    INNER JOIN [pps-distribution-datamart-db].store.entity_all_fund f2
        ON f.FundCode = f2.FundCode
    INNER JOIN [pps-distribution-datamart-db].store.entity_all_unit_holder uh2
        ON uh.UnitHolderCode = uh2.UnitHolderCode
    LEFT JOIN [pps-distribution-datamart-db].store.entity_all_broker b2
        ON b.BrokerCode = b2.BrokerCode
    LEFT JOIN [pps-distribution-datamart-db].store.entity_all_user ivs2
        ON ivs.[User] = ivs2.[User]
        AND ivs2.Role = 'Investment Specialist'
    
    WHERE si.PriceDate BETWEEN @StartDate AND @EndDate
        AND si.PortfolioSwitchDate BETWEEN DATEADD(MONTH, -1, @StartDate) AND @EndDate

END
GO
