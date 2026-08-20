// Shapes returned by the FPBudget /api/v1 endpoints (see routes/api_v1.py
// and MOBILE_APP_PLAN.md — v1 responses may gain fields but never lose them).

export interface ApiUser {
  id: number;
  email: string;
  name: string | null;
  role: string;
  display_role: string;
  is_docs_only: boolean;
}

export interface ApiProject {
  id: number;
  name: string;
  client_name: string | null;
  status: string;
  role: string; // owner | editor | viewer | docs_only
  can_upload_docs: boolean;
}

export interface MeResponse {
  user: ApiUser;
  projects: ApiProject[];
}

export interface LoginResponse {
  token: string;
  user: ApiUser;
}

// POST /api/v1/projects/<pid>/docs/upload — synchronous: OCR runs during
// the request, so vendor/amount/doc_date arrive in this same response.
export interface UploadResponse {
  status: "ok" | "review_dup" | "review" | "error";
  upload_id?: number;
  filed_filename?: string | null;
  new_filename?: string | null;
  doc_type?: string | null;
  confidence?: number | null;
  duplicate?: boolean;
  duplicate_of?: number | null;
  message?: string;
  vendor?: string | null;
  amount?: number | null;
  doc_date?: string | null;
  doc_number?: string | null;
  error?: string;
}

export interface BudgetInfo {
  id: number;
  name: string;
  budget_mode: string;
  mode_label: string;
  is_actual: boolean;
  version_status: string; // current | superseded
  version_number: number | null;
  start_date: string | null;
  end_date: string | null;
  target_budget: number | null;
  updated_at: string | null;
}

export interface SectionRow {
  code: number;
  account: string;
  estimated: number;
  actual: number;
  variance: number;
}

export interface BudgetLineData {
  id: number;
  account_code: number;
  account_name: string;
  description: string;
  is_labor: boolean;
  use_schedule: boolean;
  line_tag: string | null;
  quantity: number | null;
  days: number | null;
  rate: number | null;
  rate_type: string | null;
  est_ot: number | null;
  fringe_type: string | null;
  agent_pct: number | null;
  note: string | null;
  sort_order: number;
  parent_line_id: number | null;
  subtotal: number;
  fringe_amount: number;
  agent_amount: number;
  total: number;
}

export interface BudgetTotals {
  subtotal_estimated: number;
  subtotal_actual: number;
  company_fee: number;
  company_fee_dispersed: boolean;
  workers_comp_amount: number;
  payroll_fee_amount: number;
  production_insurance_amount: number;
  grand_total_estimated: number;
  grand_total_actual: number;
  grand_variance: number;
}

export interface BudgetSummary {
  budget: BudgetInfo;
  totals: BudgetTotals;
  sections: SectionRow[];
  lines: BudgetLineData[];
}

// Body for POST .../line — id present = update, absent = create. The server
// may 409 with {estimated_protected} (resend with override_estimated: true)
// or {schedule_conflict} (resolve on the website).
export interface LineSavePayload {
  id?: number;
  description?: string;
  note?: string | null;
  quantity?: number;
  days?: number;
  rate?: number;
  est_ot?: number;
  // Fraction on the wire (0.15 = 15%): discount on non-labor, agent fee
  // on labor — same field the web grid edits.
  agent_pct?: number;
  account_code?: number;
  account_name?: string;
  is_labor?: boolean;
  override_estimated?: boolean;
}

export interface RecentUpload {
  id: number;
  status: string;
  original_filename: string | null;
  filed_filename: string | null;
  vendor: string | null;
  amount: number | null;
  doc_date: string | null;
  is_duplicate: boolean;
  uploaded_at: string | null;
}
