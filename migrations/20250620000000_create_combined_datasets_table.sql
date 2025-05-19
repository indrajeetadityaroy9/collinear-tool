-- Create combined_datasets table
CREATE TABLE IF NOT EXISTS public.combined_datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    description TEXT,
    source_datasets TEXT[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by UUID REFERENCES auth.users(id),
    impact_level TEXT CHECK (impact_level = ANY (ARRAY['low', 'medium', 'high']::text[])),
    status TEXT NOT NULL DEFAULT 'processing',
    combination_strategy TEXT NOT NULL DEFAULT 'merge',
    size_bytes BIGINT,
    file_count INTEGER,
    downloads INTEGER,
    likes INTEGER
);

-- Add indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_combined_datasets_created_by ON public.combined_datasets(created_by);
CREATE INDEX IF NOT EXISTS idx_combined_datasets_impact_level ON public.combined_datasets(impact_level);
CREATE INDEX IF NOT EXISTS idx_combined_datasets_status ON public.combined_datasets(status);

-- Add Row Level Security (RLS) policies
ALTER TABLE public.combined_datasets ENABLE ROW LEVEL SECURITY;

-- Policy to allow users to see all combined datasets
CREATE POLICY "Anyone can view combined datasets" 
ON public.combined_datasets 
FOR SELECT USING (true);

-- Policy to allow users to create their own combined datasets
CREATE POLICY "Users can create their own combined datasets" 
ON public.combined_datasets 
FOR INSERT 
WITH CHECK (auth.uid() = created_by);

-- Policy to allow users to update only their own combined datasets
CREATE POLICY "Users can update their own combined datasets" 
ON public.combined_datasets 
FOR UPDATE 
USING (auth.uid() = created_by);

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_combined_datasets_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update updated_at timestamp
CREATE TRIGGER update_combined_datasets_updated_at_trigger
BEFORE UPDATE ON public.combined_datasets
FOR EACH ROW
EXECUTE FUNCTION update_combined_datasets_updated_at(); 