Vagrant.configure("2") do |config|
    config.vm.box = "ubuntu/xenial64"
    config.vm.provider :virtualbox do |vb| vb.name = "ndp" end

    config.vm.network "private_network", ip: "172.17.8.180"
    config.vm.network :forwarded_port, guest: 8080, host: 8080
    config.vm.synced_folder ".", "/vagrant", :nfs => true

    config.vm.provision "ansible" do |ansible|
        ansible.playbook = "ansible/provision.yaml"
    end
end
