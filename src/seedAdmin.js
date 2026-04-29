import { createClient } from '@supabase/supabase-js';
import 'dotenv/config';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('Missing Supabase credentials in .env');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

async function seedAdmin() {
  const email = 'admin@oshu.in';
  const password = 'Test@123';

  console.log(`Checking if admin user exists: ${email}`);

  // 1. Create user in auth.users
  const { data: userData, error: userError } = await supabase.auth.admin.createUser({
    email,
    password,
    email_confirm: true,
    user_metadata: { full_name: 'Oshu Admin' }
  });

  if (userError) {
    if (userError.message.includes('already registered') || userError.message.includes('already exists')) {
      console.log('Admin user already exists in auth.users. Updating password and role...');
      
      const { data: users, error: listError } = await supabase.auth.admin.listUsers();
      if (listError) {
        console.error('Error listing users:', listError);
        return;
      }
      const existingUser = users.users.find(u => u.email === email);
      if (existingUser) {
        // Update password just in case
        await supabase.auth.admin.updateUserById(existingUser.id, { password });
        await updateProfile(existingUser.id);
      }
    } else {
      console.error('Error creating admin user:', userError);
    }
  } else {
    console.log(`Admin user created successfully with ID: ${userData.user.id}`);
    await updateProfile(userData.user.id);
  }
}

async function updateProfile(userId) {
  console.log(`Updating profile for user ${userId} to admin role...`);
  
  const { data, error } = await supabase
    .from('profiles')
    .upsert({
      id: userId,
      role: 'admin',
      full_name: 'Oshu Admin'
    })
    .select();

  if (error) {
    console.error('Error updating profile:', error);
  } else {
    console.log('Profile updated successfully:', data);
  }
}

seedAdmin();
