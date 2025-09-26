using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Microsoft.Data.SqlClient;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

public class Function
{
    private readonly string _connString;

    public Function()
    {
        // Get the ARN of the secret stored in the environment variable
        string secretArn = Environment.GetEnvironmentVariable("SQL_SECRET_ARN");
        if (string.IsNullOrEmpty(secretArn))
        {
            throw new InvalidOperationException("SQL_SECRET_ARN environment variable is missing.");
        }

        // Fetch the connection string from Secrets Manager using the ARN
        _connString = GetSecretFromSecretsManager(secretArn).Result;
    }

    // Fetch secret from AWS Secrets Manager
    private static async Task<string> GetSecretFromSecretsManager(string secretArn)
    {
        using var client = new AmazonSecretsManagerClient();
        try
        {
            // Create a request to get the secret value using the ARN
            var request = new GetSecretValueRequest
            {
                SecretId = secretArn
            };
            var response = await client.GetSecretValueAsync(request);

            // Return the secret value (assuming it is stored as plain text)
            return response.SecretString;
        }
        catch (Exception ex)
        {
            // Log and rethrow the error if fetching the secret fails
            Console.WriteLine($"Error retrieving secret: {ex.Message}");
            throw new InvalidOperationException("Unable to retrieve SQL connection string from Secrets Manager.");
        }
    }

    public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest request, ILambdaContext ctx)
    {
        try
        {
            var input = JsonSerializer.Deserialize<VendorInvoice>(request.Body ?? "{}") ?? new VendorInvoice();
            var missing = new List<string>();
            if (string.IsNullOrWhiteSpace(input.t_idno)) missing.Add(nameof(input.t_idno));
            if (string.IsNullOrWhiteSpace(input.t_amti)) missing.Add(nameof(input.t_amti));
            if (string.IsNullOrWhiteSpace(input.t_orno)) missing.Add(nameof(input.t_orno));
            if (string.IsNullOrWhiteSpace(input.t_cprj)) missing.Add(nameof(input.t_cprj));
            if (string.IsNullOrWhiteSpace(input.t_ccur)) missing.Add(nameof(input.t_ccur));
            if (string.IsNullOrWhiteSpace(input.t_cpay)) missing.Add(nameof(input.t_cpay));
            if (string.IsNullOrWhiteSpace(input.t_ptyp)) missing.Add(nameof(input.t_ptyp));
            if (missing.Count > 0) return Bad(400, $"Missing: {string.Join(", ", missing)}");

            if (!double.TryParse(input.t_amti, out var amt) || amt <= 0) return Bad(400, "t_amti must be > 0.");

            // TODO: replace with real validations:
            if (string.IsNullOrWhiteSpace(input.t_ifbp)) return Bad(400, "This Business Partner does not exist");
            if (string.IsNullOrWhiteSpace(input.t_cprj)) return Bad(400, "This Cost Center is not in the ERP");

            string action;
            using var conn = new SqlConnection(_connString);
            await conn.OpenAsync();

            var exists = await Exists(conn, input.t_idno);
            if (!exists) { await Insert(conn, input); action = "INSERTED"; }
            else         { await Update(conn, input); action = "UPDATED"; }

            return Ok(new { action, affectedRecords = 1 });
        }
        catch (Exception ex)
        {
            return Bad(500, ex.Message);
        }
    }

    private static async Task<bool> Exists(SqlConnection c, string id)
    {
        using var cmd = new SqlCommand("SELECT 1 FROM vendor_invoice WHERE t_idno=@id", c);
        cmd.Parameters.AddWithValue("@id", id);
        return (await cmd.ExecuteScalarAsync()) != null;
    }

    private static async Task Insert(SqlConnection c, VendorInvoice r)
    {
        var sql = @"
INSERT INTO vendor_invoice 
 (t_ninv,t_ifbp,t_isup,t_invd,t_ccur,t_amth_1,t_amth_2,t_amth_3,t_amti,t_refr,t_cpay,t_stin,t_paym,
  t_dim1,t_dim2,t_dim3,t_dim4,t_dim5,t_bkrn,t_bank,t_orno,t_cprj,t_Refcntd,t_Refcntu,t_idno,t_sync,
  t_ptyp,t_rrmk,t_srmk,t_sydt,t_cncl,t_cndt,t_updt,t_udat)
VALUES
 (@t_ninv,@t_ifbp,@t_isup,@t_invd,@t_ccur,@t_amth_1,@t_amth_2,@t_amth_3,@t_amti,@t_refr,@t_cpay,@t_stin,@t_paym,
  @t_dim1,@t_dim2,@t_dim3,@t_dim4,@t_dim5,@t_bkrn,@t_bank,@t_orno,@t_cprj,@t_Refcntd,@t_Refcntu,@t_idno,@t_sync,
  @t_ptyp,@t_rrmk,' ',GETDATE(),2,GETDATE(),2,GETDATE());";
        using var cmd = new SqlCommand(sql, c);
        Map(cmd, r);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task Update(SqlConnection c, VendorInvoice r)
    {
        var sql = @"
UPDATE vendor_invoice SET
 t_ifbp=@t_ifbp,t_isup=@t_isup,t_invd=@t_invd,t_ccur=@t_ccur,t_amth_1=@t_amth_1,t_amth_2=@t_amth_2,t_amth_3=@t_amth_3,
 t_amti=@t_amti,t_refr=@t_refr,t_cpay=@t_cpay,t_stin=@t_stin,t_paym=@t_paym,t_dim1=@t_dim1,t_dim2=@t_dim2,t_dim3=@t_dim3,
 t_dim4=@t_dim4,t_dim5=@t_dim5,t_bkrn=@t_bkrn,t_bank=@t_bank,t_orno=@t_orno,t_cprj=@t_cprj,t_Refcntd=@t_Refcntd,
 t_Refcntu=@t_Refcntu,t_sync=@t_sync,t_ptyp=@t_ptyp,t_rrmk=@t_rrmk,t_updt=2,t_udat=GETDATE()
WHERE t_idno=@t_idno;";
        using var cmd = new SqlCommand(sql, c);
        Map(cmd, r);
        await cmd.ExecuteNonQueryAsync();
    }

    private static void Map(SqlCommand cmd, VendorInvoice r)
    {
        cmd.Parameters.AddWithValue("@t_ninv", r.t_ninv ?? "0");
        cmd.Parameters.AddWithValue("@t_ifbp", r.t_ifbp ?? "");
        cmd.Parameters.AddWithValue("@t_isup", r.t_isup ?? "");
        cmd.Parameters.AddWithValue("@t_invd", r.t_invd ?? "");
        cmd.Parameters.AddWithValue("@t_ccur", r.t_ccur ?? "");
        cmd.Parameters.AddWithValue("@t_amth_1", r.t_amth_1 ?? "0");
        cmd.Parameters.AddWithValue("@t_amth_2", r.t_amth_2 ?? "0");
        cmd.Parameters.AddWithValue("@t_amth_3", r.t_amth_3 ?? "0");
        cmd.Parameters.AddWithValue("@t_amti", r.t_amti ?? "0");
        cmd.Parameters.AddWithValue("@t_refr", r.t_refr ?? "");
        cmd.Parameters.AddWithValue("@t_cpay", r.t_cpay ?? "");
        cmd.Parameters.AddWithValue("@t_stin", r.t_stin ?? "1");
        cmd.Parameters.AddWithValue("@t_paym", r.t_paym ?? "");
        cmd.Parameters.AddWithValue("@t_dim1", string.IsNullOrEmpty(r.t_dim1) ? (r.t_cprj ?? "") : r.t_dim1);
        cmd.Parameters.AddWithValue("@t_dim2", r.t_dim2 ?? "");
        cmd.Parameters.AddWithValue("@t_dim3", r.t_dim3 ?? "");
        cmd.Parameters.AddWithValue("@t_dim4", r.t_dim4 ?? "");
        cmd.Parameters.AddWithValue("@t_dim5", r.t_dim5 ?? "");
        cmd.Parameters.AddWithValue("@t_bkrn", r.t_bkrn ?? "");
        cmd.Parameters.AddWithValue("@t_bank", r.t_bank ?? "");
        cmd.Parameters.AddWithValue("@t_orno", r.t_orno ?? "");
        cmd.Parameters.AddWithValue("@t_cprj", r.t_cprj ?? "");
        cmd.Parameters.AddWithValue("@t_Refcntd", r.t_Refcntd ?? "0");
        cmd.Parameters.AddWithValue("@t_Refcntu", r.t_Refcntu ?? "0");
        cmd.Parameters.AddWithValue("@t_idno", r.t_idno ?? "");
        cmd.Parameters.AddWithValue("@t_sync", r.t_sync ?? "2");
        cmd.Parameters.AddWithValue("@t_ptyp", r.t_ptyp ?? "");
        cmd.Parameters.AddWithValue("@t_rrmk", r.t_rrmk ?? "");
    }

    private static APIGatewayProxyResponse Ok(object o) =>
        new() {
            StatusCode = 200,
            Body = JsonSerializer.Serialize(o),
            Headers = new Dictionary<string,string> { ["Content-Type"] = "application/json" }
        };

    private static APIGatewayProxyResponse Bad(int code, string msg) =>
        new() {
            StatusCode = code,
            Body = JsonSerializer.Serialize(new { error = msg }),
            Headers = new Dictionary<string,string> { ["Content-Type"] = "application/json" }
        };

    public class VendorInvoice
    {
        public string t_ninv { get; set; } = "0";
        public string t_ifbp { get; set; } = "";
        public string t_isup { get; set; } = "";
        public string t_invd { get; set; } = "";
        public string t_ccur { get; set; } = "";
        public string t_amth_1 { get; set; } = "0";
        public string t_amth_2 { get; set; } = "0";
        public string t_amth_3 { get; set; } = "0";
        public string t_amti { get; set; } = "";
        public string t_refr { get; set; } = "";
        public string t_cpay { get; set; } = "";
        public string t_stin { get; set; } = "1";
        public string t_paym { get; set; } = "";
        public string t_cprj { get; set; } = "";
        public string t_dim1 { get; set; } = "";
        public string t_dim2 { get; set; } = "";
        public string t_dim3 { get; set; } = "";
        public string t_dim4 { get; set; } = "";
        public string t_dim5 { get; set; } = "";
        public string t_bkrn { get; set; } = "";
        public string t_bank { get; set; } = "";
        public string t_orno { get; set; } = "";
        public string t_Refcntd { get; set; } = "0";
        public string t_Refcntu { get; set; } = "0";
        public string t_idno { get; set; } = "";
        public string t_sync { get; set; } = "2";
        public string t_ptyp { get; set; } = "";
        public string t_rrmk { get; set; } = "";
    }
}

