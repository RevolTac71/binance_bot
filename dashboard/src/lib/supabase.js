import { createClient } from "@supabase/supabase-js";

// These should be set in .env.local
const supabaseUrl =
  import.meta.env.VITE_SUPABASE_URL ||
  "https://uvqhpiilmameyjortqoc.supabase.co";
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY || "";

export const supabase = createClient(supabaseUrl, supabaseAnonKey);
